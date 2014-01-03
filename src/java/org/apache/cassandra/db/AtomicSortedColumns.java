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

import java.util.*;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

import com.google.common.base.Function;
import com.google.common.collect.Iterables;

import edu.stanford.ppl.concurrent.SnapTreeMap;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.db.composites.CellName;
import org.apache.cassandra.db.composites.CellNameType;
import org.apache.cassandra.db.filter.ColumnSlice;
import org.apache.cassandra.db.index.SecondaryIndexManager;
import org.apache.cassandra.utils.memory.Allocator;

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
public class AtomicSortedColumns extends ColumnFamily
{
    private volatile Holder ref;
    private static final AtomicReferenceFieldUpdater<AtomicSortedColumns, Holder> refUpdater
            = AtomicReferenceFieldUpdater.newUpdater(AtomicSortedColumns.class, Holder.class, "ref");

    public static final ColumnFamily.Factory<AtomicSortedColumns> factory = new Factory<AtomicSortedColumns>()
    {
        public AtomicSortedColumns create(CFMetaData metadata, boolean insertReversed)
        {
            return new AtomicSortedColumns(metadata);
        }
    };

    private AtomicSortedColumns(CFMetaData metadata)
    {
        this(metadata, new Holder(metadata.comparator));
    }

    private AtomicSortedColumns(CFMetaData metadata, Holder holder)
    {
        super(metadata);
        this.ref = holder;
    }

    public CellNameType getComparator()
    {
        return (CellNameType)ref.map.comparator();
    }

    public ColumnFamily.Factory getFactory()
    {
        return factory;
    }

    public ColumnFamily cloneMe()
    {
        return new AtomicSortedColumns(metadata, ref.cloneMe());
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

    public void purgeTombstones(int gcBefore)
    {
        while (true)
        {
            Holder current = ref;
            if (!current.deletionInfo.hasPurgeableTombstones(gcBefore))
                break;

            DeletionInfo purgedInfo = current.deletionInfo.copy();
            purgedInfo.purge(gcBefore);
            if (refUpdater.compareAndSet(this, current, current.with(purgedInfo)))
                break;
        }
    }

    public void addColumn(Cell cell, Allocator allocator)
    {
        Holder current, modified;
        do
        {
            current = ref;
            modified = current.cloneMe();
            modified.addColumn(cell, allocator, SecondaryIndexManager.nullUpdater, new Delta());
        }
        while (!refUpdater.compareAndSet(this, current, modified));
    }

    public void addAll(ColumnFamily cm, Allocator allocator, Function<Cell, Cell> transformation)
    {
        addAllWithSizeDelta(cm, allocator, transformation, SecondaryIndexManager.nullUpdater, new Delta());
    }

    /**
     *  This is only called by Memtable.resolve, so only AtomicSortedColumns needs to implement it.
     *
     *  @return the difference in size seen after merging the given columns
     */
    public Delta addAllWithSizeDelta(ColumnFamily cm, Allocator allocator, Function<Cell, Cell> transformation, SecondaryIndexManager.Updater indexer, Delta delta)
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
        Holder current, modified;

        main_loop:
        while (true)
        {
            current = ref;
            delta.reset(current.map.size());
            DeletionInfo newDelInfo = current.deletionInfo.copy().add(cm.deletionInfo());
            delta.addHeapSize(newDelInfo.excessHeapSize() - current.deletionInfo.excessHeapSize());
            modified = new Holder(current.map.clone(), newDelInfo);

            if (cm.deletionInfo().hasRanges())
            {
                for (Cell currentCell : Iterables.concat(current.map.values(), cm))
                {
                    if (cm.deletionInfo().isDeleted(currentCell))
                        indexer.remove(currentCell);
                }
            }

            // TODO : if we fail we should always free up any space we allocated. This can wait until #6271
            for (Cell cell : cm)
            {
                modified.addColumn(transformation.apply(cell), allocator, indexer, delta);
                // bail early if we know we've been beaten
                if (ref != current)
                    continue main_loop;
            }

            if (refUpdater.compareAndSet(this, current, modified))
                break;
        }

        indexer.updateRowLevelIndexes();

        return delta;
    }

    public boolean replace(Cell oldCell, Cell newCell)
    {
        if (!oldCell.name().equals(newCell.name()))
            throw new IllegalArgumentException();

        Holder current, modified;
        boolean replaced;
        do
        {
            current = ref;
            modified = current.cloneMe();
            replaced = modified.map.replace(oldCell.name(), oldCell, newCell);
        }
        while (!refUpdater.compareAndSet(this, current, modified));
        return replaced;
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

    public Cell getColumn(CellName name)
    {
        return ref.map.get(name);
    }

    public SortedSet<CellName> getColumnNames()
    {
        return ref.map.keySet();
    }

    public Collection<Cell> getSortedColumns()
    {
        return ref.map.values();
    }

    public Collection<Cell> getReverseSortedColumns()
    {
        return ref.map.descendingMap().values();
    }

    public int getColumnCount()
    {
        return ref.map.size();
    }

    public Iterator<Cell> iterator(ColumnSlice[] slices)
    {
        return new ColumnSlice.NavigableMapIterator(ref.map, slices);
    }

    public Iterator<Cell> reverseIterator(ColumnSlice[] slices)
    {
        return new ColumnSlice.NavigableMapIterator(ref.map.descendingMap(), slices);
    }

    public boolean isInsertReversed()
    {
        return false;
    }

    private static class Holder
    {
        // This is a small optimization: DeletionInfo is mutable, but we know that we will always copy it in that class,
        // so we can safely alias one DeletionInfo.live() reference and avoid some allocations.
        private static final DeletionInfo LIVE = DeletionInfo.live();

        final SnapTreeMap<CellName, Cell> map;
        final DeletionInfo deletionInfo;

        Holder(CellNameType comparator)
        {
            this(new SnapTreeMap<CellName, Cell>(comparator), LIVE);
        }

        Holder(SnapTreeMap<CellName, Cell> map, DeletionInfo deletionInfo)
        {
            this.map = map;
            this.deletionInfo = deletionInfo;
        }

        Holder cloneMe()
        {
            return with(map.clone());
        }

        Holder with(DeletionInfo info)
        {
            return new Holder(map, info);
        }

        Holder with(SnapTreeMap<CellName, Cell> newMap)
        {
            return new Holder(newMap, deletionInfo);
        }

        // There is no point in cloning the underlying map to clear it
        // afterwards.
        Holder clear()
        {
            return new Holder(new SnapTreeMap<CellName, Cell>(map.comparator()), LIVE);
        }

        void addColumn(Cell cell, Allocator allocator, SecondaryIndexManager.Updater indexer, Delta delta)
        {
            CellName name = cell.name();
            while (true)
            {
                Cell oldCell = map.putIfAbsent(name, cell);
                if (oldCell == null)
                {
                    indexer.insert(cell);
                    delta.insert(cell);
                    return;
                }

                Cell reconciledCell = cell.reconcile(oldCell, allocator);
                if (oldCell == reconciledCell)
                {
                    // when compacting we need to make sure we update indexes no matter the order we merge
                    indexer.update(cell, reconciledCell);
                    return;
                }
                if (map.replace(name, oldCell, reconciledCell))
                {
                    indexer.update(oldCell, reconciledCell);
                    delta.swap(oldCell, reconciledCell);
                    return;
                }
                // We failed to replace cell due to a concurrent update or a concurrent removal. Keep trying.
                // (Currently, concurrent removal should not happen (only updates), but let us support that anyway.)
            }
        }
    }


    // TODO: create a stack-allocation-friendly list to help optimise garbage for updates to rows with few columns
    public static final class Delta
    {
        private long dataSize;
        private long heapSize;
        private int oldColumnCount;
        private int newColumnCount;
        private final List<Cell> discarded = new ArrayList<>();
        protected void reset(int oldColumnCount)
        {
            this.dataSize = 0;
            this.heapSize = 0;
            this.newColumnCount = oldColumnCount;
            this.oldColumnCount = oldColumnCount;
            discarded.clear();
        }
        protected void addHeapSize(long heapSize)
        {
            this.heapSize += heapSize;
        }
        protected void swap(Cell old, Cell upd)
        {
            dataSize += upd.dataSize() - old.dataSize();
            heapSize += upd.sizeOnHeapWithoutDataBytes() - old.sizeOnHeapWithoutDataBytes();
            discarded.add(old);
        }
        protected void insert(Cell insert)
        {
            this.dataSize += insert.dataSize();
            this.heapSize += insert.sizeOnHeapWithoutDataBytes();
            this.newColumnCount += 1;
        }

        public long dataSize()
        {
            return dataSize;
        }

        public long excessHeapSize()
        {
            return heapSize;
        }

        public int newColumnCount()
        {
            return newColumnCount;
        }

        public int oldColumnCount()
        {
            return oldColumnCount;
        }

        public List<Cell> discarded()
        {
            return discarded;
        }

    }

}
