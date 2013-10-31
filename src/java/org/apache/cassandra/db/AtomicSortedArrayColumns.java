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
import edu.stanford.ppl.concurrent.SnapTreeMap;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.db.filter.ColumnSlice;
import org.apache.cassandra.db.index.SecondaryIndexManager;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.utils.Allocator;
import org.apache.cassandra.utils.ImmutableSortedArrayMap;
import org.apache.cassandra.utils.Pair;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.util.AbstractCollection;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.SortedSet;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

/**
 * A thread-safe and atomic ISortedColumns implementation.
 * Operations (in particular addAll) on this implemenation are atomic and
 * isolated (in the sense of ACID). Typically a addAll is guaranteed that no
 * other thread can see the state where only parts but not all columns have
 * been added.
 *
 * The implementation uses snaptree (https://github.com/nbronson/snaptree),
 * and in particular it's copy-on-write clone operation to achieve its
 * atomicity guarantee.
 *
 * WARNING: removing element through getSortedColumns().iterator() is *not*
 * isolated of other operations and could actually be fully ignored in the
 * face of a concurrent. Don't use it unless in a non-concurrent context.
 */
public class AtomicSortedArrayColumns extends ColumnFamily
{
    private volatile Holder ref;

    public static final Factory<AtomicSortedArrayColumns> factory = new Factory<AtomicSortedArrayColumns>()
    {
        public AtomicSortedArrayColumns create(CFMetaData metadata, boolean insertReversed)
        {
            if (insertReversed)
                throw new IllegalArgumentException();
            return new AtomicSortedArrayColumns(metadata);
        }
    };

    private AtomicSortedArrayColumns(CFMetaData metadata)
    {
        this(metadata, new Holder(metadata.comparator));
    }

    private AtomicSortedArrayColumns(CFMetaData metadata, Holder holder)
    {
        super(metadata);
        this.ref = holder;
    }

    public AbstractType<?> getComparator()
    {
        return ref.comparator;
    }

    public Factory getFactory()
    {
        return factory;
    }

    public ColumnFamily cloneMe()
    {
        return new AtomicSortedArrayColumns(metadata, ref);
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
        Holder update = new Holder(new Column[] { column }, 1, ref.comparator);
        while (ref.update(update, allocator, SecondaryIndexManager.nullUpdater, this) == Long.MIN_VALUE);
    }

    public void addAll(ColumnFamily cm, Allocator allocator, Function<Column, Column> transformation)
    {
        addAllWithSizeDelta(cm, allocator, transformation, SecondaryIndexManager.nullUpdater);
    }

    /**
     *  This is only called by Memtable.resolve, so only AtomicSortedColumns needs to implement it.
     *
     *  @return the difference in size seen after merging the given columns
     */
    public long addAllWithSizeDelta(ColumnFamily cm, Allocator allocator, Function<Column, Column> transformation, SecondaryIndexManager.Updater indexer)
    {
        /*
         * This operation needs to atomicity and isolation. To that end, we
         * add the new column to a copy of the map (a cheap O(1) snapTree
         * clone) and atomically compare and swap when everything has been
         * added. Of course, we must not forget to update the deletion times
         * too.
         * In case we are adding a lot of columns, failing the final compare
         * and swap could be expensive. To mitigate, we check we haven't been
         * beaten by another thread after every column addition. If we have,
         * we bail early, avoiding unnecessary work if possible.
         */
        Column[] trg1, trg;
        trg1 = new Column[16];
        if (cm.getColumnCount() < 16)
            trg = trg1;
        else
            trg = new Column[cm.getColumnCount()];

        int c = 0;
        if (cm instanceof UnsortedColumns)
        {
            for (Column col : cm)
            {
                if (cm.deletionInfo().isDeleted(col))
                    indexer.remove(col);
                trg[c++] = transformation.apply(col);
            }
            final Comparator<ByteBuffer> cmp = ref.comparator;
            Arrays.sort(trg, 0, c, new Comparator<Column>()
            {
                @Override
                public int compare(Column o1, Column o2)
                {
                    return cmp.compare(o1.name, o2.name);
                }
            });
        }
        else
        {
            for (Column col : cm.getSortedColumns())
            {
                if (cm.deletionInfo().isDeleted(col))
                    indexer.remove(col);
                trg[c++] = transformation.apply(col);
            }
        }

        Holder update = new Holder(trg, c, ref.comparator, cm.deletionInfo());

        while (true)
        {
            Holder current = ref;
            if (update.deletionInfo.hasRanges())
                for (Column col : current.columns)
                    if (update.deletionInfo.isDeleted(col))
                        indexer.remove(col);

            long delta = current.update(update, allocator, indexer, this);

            if (delta > Long.MIN_VALUE)
            {
                indexer.updateRowLevelIndexes();
                return delta;
            }
        }

    }

    public boolean replace(Column oldColumn, Column newColumn)
    {
        if (!oldColumn.name().equals(newColumn.name()))
            throw new IllegalArgumentException();

        while (true)
        {
            Holder cur = ref, mod = cur.replace(newColumn);
            if (mod == cur)
                return false;
            if (refUpdater.compareAndSet(this, cur, mod))
                return true;
        }
    }

    public void clear()
    {
        Holder current, modified;
        do
        {
            current = ref;
            modified = current.clear();
        }
        while (!refUpdater.compareAndSet(this, current, modified));
    }

    public Column getColumn(ByteBuffer name)
    {
        return ref.get(name);
    }

    public Iterable<ByteBuffer> getColumnNames()
    {
        return ref.keys(true);
    }

    public Collection<Column> getSortedColumns()
    {
        return ref.columns(true);
    }

    public Collection<Column> getReverseSortedColumns()
    {
        return ref.columns(false);
    }

    public int getColumnCount()
    {
        return ref.columns.length;
    }

    public Iterator<Column> iterator(ColumnSlice[] slices)
    {
        return ref.slice(slices, true);
    }

    public Iterator<Column> reverseIterator(ColumnSlice[] slices)
    {
        return ref.slice(slices, false);
    }

    public boolean isInsertReversed()
    {
        return false;
    }

    private static class Holder
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

        // This is a small optimization: DeletionInfo is mutable, but we know that we will always copy it in that class,
        // so we can safely alias one DeletionInfo.live() reference and avoid some allocations.
        private static final Column[] EMPTY = new Column[0];
        private static final DeletionInfo LIVE = DeletionInfo.live();
        final DeletionInfo deletionInfo;
        final AbstractType<?> comparator;
        final Column[] columns;
        final int length;

        Holder(AbstractType<?> comparator)
        {
            this.comparator = comparator;
            this.deletionInfo = LIVE;
            this.columns = EMPTY;
            this.length = 0;
        }

        Holder(Column[] columns, int length, AbstractType<?> comparator)
        {
            this(columns, length, comparator, LIVE);
        }
        Holder(Column[] columns, int length, AbstractType<?> comparator, DeletionInfo deletionInfo)
        {
            this.comparator = comparator;
            this.columns = columns;
            this.deletionInfo = deletionInfo;
            this.length = length;
        }

        Holder(Holder copy, DeletionInfo deletionInfo)
        {
            this.comparator = copy.comparator;
            this.columns = copy.columns;
            this.length = copy.length;
            this.deletionInfo = deletionInfo;
        }

        Holder with(DeletionInfo info)
        {
            return new Holder(this, info);
        }

        Iterator<Column> slice(ColumnSlice[] slices, boolean asc)
        {
            List<Column> list = Arrays.asList(columns);
            if (length < columns.length)
                list = list.subList(0, length);
            return new ArrayBackedSortedColumns.SlicesIterator(list, comparator, slices, !asc);
        }

        // There is no point in cloning the underlying map to clear it
        // afterwards.
        Holder clear()
        {
            return new Holder(comparator);
        }

        private static final int STACK_ALLOC_ARRAY_LEN = 16;

        // _that_ should be disposable, i.e. can be modified in place
        public long update(Holder update, Allocator allocator, SecondaryIndexManager.Updater indexer, AtomicSortedArrayColumns parent)
        {
            Holder old = this;
            Column[] trg1 = new Column[STACK_ALLOC_ARRAY_LEN], r;
            int maxLen = old.length + update.length;
            if (maxLen > STACK_ALLOC_ARRAY_LEN)
                r = new Column[maxLen];
            else
                r = trg1;

            long delta = 0;
            int i1 = 0, i2 = 0, ir = 0;
            while (i1 < old.length && i2 < update.length)
            {

                if (((i1 + i2) & 15) == 15 && parent.ref != this)
                    return Long.MIN_VALUE;

                Column oldCol = old.columns[i1];
                Column replaceCol = update.columns[i2];
                int c = comparator.compare(oldCol.name, replaceCol.name);
                if (c >= 0)
                {
                    i2++;
                    if (c == 0)
                    {
                        i1++;
                        Column reconciledCol = replaceCol.reconcile(oldCol, allocator);
                        r[ir++] = reconciledCol;
                        if (reconciledCol == replaceCol)
                            indexer.update(oldCol, reconciledCol);
                        else
                            indexer.update(replaceCol, reconciledCol);
                        delta += reconciledCol.dataSize() - oldCol.dataSize();
                    }
                    else
                    {
                        r[ir++] = replaceCol;
                        indexer.insert(replaceCol);
                        delta += replaceCol.dataSize();
                    }
                }
                else
                {
                    r[ir++] = oldCol;
                    i1++;
                }
            }

            if (i1 < old.length)
            {
                System.arraycopy(this.columns, i1, r, ir, this.length - i1);
                ir += this.length - i1;
            }
            else if (i2 < update.columns.length)
            {
                System.arraycopy(update.columns, i2, r, ir, update.length - i2);
                ir += update.length - i2;
                while (i2 < update.length)
                {
                    indexer.insert(update.columns[i2]);
                    delta += update.columns[i2].dataSize();
                    i2++;
                }
            }

            Holder updateHolder = new Holder(
                    Arrays.copyOf(r, ir), ir, comparator,
                    deletionInfo.copy().add(update.deletionInfo)
            );

            if (refUpdater.compareAndSet(parent, this, updateHolder))
                return delta;

            return Long.MIN_VALUE;
        }

        public Holder replace(Column column)
        {
            int i = equal(comparator, columns, column.name, 0, columns.length);
            if (i < 0)
                return this;
            Column[] columns = this.columns.clone();
            columns[i] = column;
            return new Holder(columns, length, comparator, deletionInfo);
        }

        public Column get(ByteBuffer key)
        {
            int i = index(key);
            return i < 0 ? null : columns[i];
        }

        public Collection<Column> columns(boolean keyAsc)
        {
            return keyAsc
                    ? this.collection(0, 1, Functions.<Column>identity())
                    : this.collection(columns.length -1, -1, Functions.<Column>identity());
        }

        public Collection<ByteBuffer> keys(boolean keyAsc)
        {
            return keyAsc
                    ? this.collection(0, 1, NAME)
                    : this.collection(columns.length -1, -1, NAME);
        }

        private <T> Collection<T> collection(final int start, final int delta, final Function<Column, T> f)
        {
            return new AbstractCollection<T>()
            {
                @Override
                public Iterator<T> iterator()
                {
                    return new Iterator<T>()
                    {
                        int p = start;
                        @Override
                        public boolean hasNext()
                        {
                            return p >= 0 && p < columns.length;
                        }

                        @Override
                        public T next()
                        {
                            T r = f.apply(columns[p]);
                            p += delta;
                            return r;
                        }

                        @Override
                        public void remove()
                        {
                            throw new UnsupportedOperationException();
                        }
                    };
                }

                @Override
                public int size()
                {
                    return columns.length;
                }
            };
        }

        public int index(ByteBuffer key)
        {
            return equal(comparator, columns, key, 0, columns.length);
        }

        private static int equal(final Comparator<ByteBuffer> comparator, final Column[] columns, final ByteBuffer key, final int fromIndex, final int toIndex) {

            int i = fromIndex - 1;
            int j = toIndex;
            // a[-1] ^= -infinity

            while (i < j - 1) {

                // { a[i] <= v ^ a[j] > v }

                final int m = (i + j) >>> 1;
                final ByteBuffer k = columns[m].name;

                int c = comparator.compare(k, key);
                if (c < 0) i = m;
                else if (c == 0) return m;
                else j = m;

                // { a[m] > v  =>        a[j] > v        =>      a[i] <= v ^ a[j] > v }
                // { a[m] <= v =>        a[i] <= v       =>      a[i] <= v ^ a[j] > v }

            }

            return -1;
        }

    }

    private static final AtomicReferenceFieldUpdater<AtomicSortedArrayColumns, Holder> refUpdater = AtomicReferenceFieldUpdater.newUpdater(AtomicSortedArrayColumns.class, Holder.class, "ref");

}
