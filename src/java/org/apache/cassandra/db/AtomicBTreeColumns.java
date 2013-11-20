/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.cassandra.db;

import com.google.common.base.*;
import com.google.common.collect.*;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.db.filter.ColumnSlice;
import org.apache.cassandra.db.index.SecondaryIndexManager;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.utils.Allocator;
import org.apache.cassandra.utils.btree.BTree;
import org.apache.cassandra.utils.btree.BTreeSet;
import org.apache.cassandra.utils.btree.ReplaceFunction;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.util.AbstractCollection;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.NavigableMap;
import java.util.NavigableSet;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

import static org.apache.cassandra.db.index.SecondaryIndexManager.Updater;

/**
 * A thread-safe and atomic ISortedColumns implementation.
 * Operations (in particular addAll) on this implemenation are atomic and
 * isolated (in the sense of ACID). Typically a addAll is guaranteed that no
 * other thread can see the state where only parts but not all columns have
 * been added.
 *
 * WARNING: removing element through getSortedColumns().iterator() is *not*
 * isolated of other operations and could actually be fully ignored in the
 * face of a concurrent. Don't use it unless in a non-concurrent context.
 */
public class AtomicBTreeColumns extends ColumnFamily
{

    private static final Function<Column, ByteBuffer> NAME = new Function<Column, ByteBuffer>()
    {
        @Nullable
        @Override
        public ByteBuffer apply(@Nullable Column column)
        {
            return column.name;
        }
    };

    public static final Factory<AtomicBTreeColumns> factory = new Factory<AtomicBTreeColumns>()
    {
        public AtomicBTreeColumns create(CFMetaData metadata, boolean insertReversed)
        {
            if (insertReversed)
                throw new IllegalArgumentException();
            return new AtomicBTreeColumns(metadata);
        }
    };

    private static final DeletionInfo LIVE = DeletionInfo.live();
    private static final Holder EMPTY = new Holder(BTree.empty(), LIVE);

    private volatile Holder ref;

    private AtomicBTreeColumns(CFMetaData metadata)
    {
        this(metadata, EMPTY);
    }

    private AtomicBTreeColumns(CFMetaData metadata, Holder holder)
    {
        super(metadata);
        this.ref = holder;
    }

    public AbstractType<?> getComparator()
    {
        return metadata.comparator;
    }

    public Factory getFactory()
    {
        return factory;
    }

    public ColumnFamily cloneMe()
    {
        return new AtomicBTreeColumns(metadata, ref);
    }

    public DeletionInfo deletionInfo()
    {
        return ref.deletionInfo;
    }

    public void delete(DeletionTime delTime)
    {
        delete(new DeletionInfo(delTime));
    }

    protected void delete(RangeTombstone tombstone)
    {
        delete(new DeletionInfo(tombstone, getComparator()));
    }

    public void delete(DeletionInfo info)
    {
        if (info.isLive())
            return;

        // Keeping deletion info for max markedForDeleteAt value
        while (true)
        {
            Holder current = ref;
            DeletionInfo newDelInfo = current.deletionInfo.copy().add(info);
            if (refUpdater.compareAndSet(this, current, current.with(newDelInfo)))
                break;
        }
    }

    public void setDeletionInfo(DeletionInfo newInfo)
    {
        ref = ref.with(newInfo);
    }

    public void maybeResetDeletionTimes(int gcBefore)
    {
        while (true)
        {
            Holder current = ref;
            if (!current.deletionInfo.hasIrrelevantData(gcBefore))
                break;

            DeletionInfo purgedInfo = current.deletionInfo.copy();
            purgedInfo.purge(gcBefore);
            if (refUpdater.compareAndSet(this, current, current.with(purgedInfo)))
                break;
        }
    }

    public void addColumn(Column column, Allocator allocator)
    {
        while (true)
        {
            Holder current = ref;
            Holder update = ref.update(this, metadata.comparator.columnComparator, Arrays.asList(column), null);
            if (refUpdater.compareAndSet(this, current, update))
                return;
        }
    }

    public void addAll(ColumnFamily cm, Allocator allocator, Function<Column, Column> transformation)
    {
        addAllWithSizeDelta(cm, allocator, transformation, SecondaryIndexManager.nullUpdater);
    }

    private static final class ColumnReplacer implements ReplaceFunction<Column>
    {
        final Allocator allocator;
        final Function<Column, Column> transform;
        final Updater indexer;
        long delta;
        // this only works on the assumption that transform allocates a new column; for now this is true
        // wherever the return value is used, but care should be taken if that changes, or the method
        // should be modified to prevent alternative uses of transformation
        long wasted;

        private ColumnReplacer(Allocator allocator, Function<Column, Column> transform, Updater indexer)
        {
            this.allocator = allocator;
            this.transform = transform;
            this.indexer = indexer;
        }

        @Override
        public Column apply(Column replaced, Column update)
        {
            if (replaced == null)
            {
                indexer.insert(update);
                delta += update.dataSize();
            }
            else
            {
                Column reconciled = update.reconcile(replaced, allocator);
                if (reconciled == update)
                    indexer.update(replaced, reconciled);
                else
                    indexer.update(update, reconciled);
                delta += reconciled.dataSize() - replaced.dataSize();
            }

            Column r = transform.apply(update);
            wasted += r.value.remaining();
            return r;
        }
    }

    private static Collection<Column> transform(Comparator<Column> cmp, ColumnFamily cf, Function<Column, Column> transformation, boolean sort)
    {
        Column[] tmp = new Column[cf.getColumnCount()];

        int i = 0;
        for (Column c : cf)
            tmp[i++] = transformation.apply(c);

        if (sort)
            Arrays.sort(tmp, cmp);

        return Arrays.asList(tmp);
    }

    /**
     *  This is only called by Memtable.resolve, so only AtomicSortedColumns needs to implement it.
     *
     *  @return the difference in size seen after merging the given columns
     */
    public long addAllWithSizeDelta(final ColumnFamily cm, Allocator allocator, Function<Column, Column> transformation, Updater indexer)
    {
        boolean transformed = false;
        Collection<Column> insert;
        if (cm instanceof UnsortedColumns)
        {
            insert = transform(metadata.comparator.columnComparator, cm, transformation, true);
            transformed = true;
        }
        else
            insert = cm.getSortedColumns();

        // run the indexer.remove() up-front to save time in the contended spot
        DeletionInfo deletionInfo = cm.deletionInfo();

        // After failing once, transform Columns into a new collection to avoid repeatedly allocating Slab space
        long wasted = 0;
        while (true)
        {
            Holder current = ref;
            if (deletionInfo.hasRanges())
            {
                for (Iterator<Column> iter : new Iterator[] { insert.iterator(), BTree.<Column>slice(current.tree, true) })
                {
                    while (iter.hasNext())
                    {
                        Column col = iter.next();
                        if (deletionInfo.isDeleted(col))
                            indexer.remove(col);
                    }
                }
            }

            ColumnReplacer replacer = new ColumnReplacer(allocator, transformation, indexer);
            Holder h = current.update(this, metadata.comparator.columnComparator, insert, replacer);
            if (h != null && refUpdater.compareAndSet(this, current, h))
            {
                indexer.updateRowLevelIndexes();
                return wasted + replacer.delta;
            }

            if (!transformed)
            {
                wasted = replacer.wasted;
                insert = transform(metadata.comparator.columnComparator, cm, transformation, false);
                transformed = true;
            }
        }

    }

    public boolean replace(Column oldColumn, Column newColumn)
    {
        if (!oldColumn.name().equals(newColumn.name()))
            throw new IllegalArgumentException();

        while (true)
        {
            Holder cur = ref, mod = cur.update(this, metadata.comparator.columnComparator, Arrays.asList(newColumn), null);
            if (mod == cur)
                return false;
            if (refUpdater.compareAndSet(this, cur, mod))
                return true;
        }
    }

    public void clear()
    {
        // no need to CAS, as we're just getting rid of everything
        ref = EMPTY;
    }

    private Comparator<Object> asymmetricComparator()
    {
        final Comparator<ByteBuffer> cmp = metadata.comparator;
        return new Comparator<Object>()
        {

            @Override
            public int compare(Object o1, Object o2)
            {
                return cmp.compare((ByteBuffer) o1, ((Column) o2).name);
            }
        };
    }

    public Column getColumn(ByteBuffer name)
    {
        return (Column) BTree.find(ref.tree, asymmetricComparator(), name);
    }

    public Iterable<ByteBuffer> getColumnNames()
    {
        return collection(false, NAME);
    }

    public Collection<Column> getSortedColumns()
    {
        return collection(true, Functions.<Column>identity());
    }

    public Collection<Column> getReverseSortedColumns()
    {
        return collection(false, Functions.<Column>identity());
    }

    private <V> Collection<V> collection(final boolean forwards, final Function<Column, V> f)
    {
        final Holder ref = this.ref;
        return new AbstractCollection<V>()
        {
            @Override
            public Iterator<V> iterator()
            {
                return Iterators.transform(BTree.<Column>slice(ref.tree, forwards), f);
            }

            @Override
            public int size()
            {
                return BTree.slice(ref.tree, true).count();
            }
        };
    }

    public int getColumnCount()
    {
        return BTree.slice(ref.tree, true).count();
    }

    public Iterator<Column> iterator(ColumnSlice[] slices)
    {
        return new NavigableSetIterator(
                new BTreeSet<>(ref.tree, getComparator().columnComparator),
                slices
        );
    }

    public Iterator<Column> reverseIterator(ColumnSlice[] slices)
    {
        return new NavigableSetIterator(
                new BTreeSet<>(ref.tree, getComparator().columnComparator).descendingSet(),
                slices
        );
    }

    public boolean isInsertReversed()
    {
        return false;
    }

    private static class Holder
    {

        // This is a small optimization: DeletionInfo is mutable, but we know that we will always copy it in that class,
        // so we can safely alias one DeletionInfo.live() reference and avoid some allocations.
        final DeletionInfo deletionInfo;
        final Object[] tree;

        Holder(Object[] tree)
        {
            this(tree, LIVE);
        }
        Holder(Object[] tree, DeletionInfo deletionInfo)
        {
            this.tree = tree;
            this.deletionInfo = deletionInfo;
        }

        Holder with(DeletionInfo info)
        {
            return new Holder(this.tree, info);
        }

        Holder update(AtomicBTreeColumns container, Comparator<Column> cmp, Collection<Column> update, ReplaceFunction<Column> replaceF)
        {
            Object[] r = BTree.update(tree, cmp, update, true, replaceF, new TerminateEarly(container, this));
            if (r == null)
                return null;
            return new Holder(r, deletionInfo);
        }

    }

    private static final class TerminateEarly implements Function<Object, Boolean>
    {

        final AtomicBTreeColumns columns;
        final Holder ref;
        private TerminateEarly(AtomicBTreeColumns columns, Holder ref)
        {
            this.columns = columns;
            this.ref = ref;
        }

        @Nullable
        @Override
        public Boolean apply(@Nullable Object o)
        {
            if (ref != columns.ref)
                return Boolean.TRUE;
            return Boolean.FALSE;
        }
    }

    private static class NavigableSetIterator extends AbstractIterator<Column>
    {
        private final NavigableSet<Column> set;
        private final ColumnSlice[] slices;

        private int idx = 0;
        private Iterator<Column> currentSlice;

        public NavigableSetIterator(NavigableSet<Column> set, ColumnSlice[] slices)
        {
            this.set = set;
            this.slices = slices;
        }

        protected Column computeNext()
        {
            if (currentSlice == null)
            {
                if (idx >= slices.length)
                    return endOfData();

                ColumnSlice slice = slices[idx++];
                // Note: we specialize the case of start == "" and finish = "" because it is slightly more efficient, but also they have a specific
                // meaning (namely, they always extend to the beginning/end of the range).
                if (slice.start.remaining() == 0)
                {
                    if (slice.finish.remaining() == 0)
                        currentSlice = set.iterator();
                    else
                        currentSlice = set.headSet(new Column(slice.finish), true).iterator();
                }
                else if (slice.finish.remaining() == 0)
                {
                    currentSlice = set.tailSet(new Column(slice.start), true).iterator();
                }
                else
                {
                    currentSlice = set.subSet(new Column(slice.start), true, new Column(slice.finish), true).iterator();
                }
            }

            if (currentSlice.hasNext())
                return currentSlice.next();

            currentSlice = null;
            return computeNext();
        }
    }


    private static final AtomicReferenceFieldUpdater<AtomicBTreeColumns, Holder> refUpdater = AtomicReferenceFieldUpdater.newUpdater(AtomicBTreeColumns.class, Holder.class, "ref");

}
