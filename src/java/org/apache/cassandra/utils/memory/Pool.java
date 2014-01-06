package org.apache.cassandra.utils.memory;

import org.apache.cassandra.concurrent.NamedThreadFactory;
import org.apache.cassandra.utils.concurrent.OpOrdering;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public abstract class Pool
{
    final PoolCleaner<?> cleaner;

    // the total memory used by this pool
    public final MemoryTracker onHeap;
    public final MemoryTracker offHeap;

    Pool(Setup setup)
    {
        this.cleaner = setup.cleaner(this);
        this.onHeap = setup.onHeap(this, cleaner);
        this.offHeap = setup.offHeap(this, cleaner);
        if (cleaner != null)
        {
            // start a thread to run the cleaner
            ExecutorService cleanerExec = Executors.newFixedThreadPool(1, new NamedThreadFactory(this.getClass().getSimpleName() + "Cleaner"));
            cleanerExec.execute(this.cleaner);
        }
    }

    public abstract PoolAllocator newAllocator(OpOrdering writes);

    static class Setup
    {

        final long maxOnHeapMemory;
        final long maxOffHeapMemory;
        final float cleanupThreshold;
        final Runnable cleaner;

        public Setup(long maxOnHeapMemory, long maxOffHeapMemory, float cleanupThreshold, Runnable cleaner)
        {
            this.maxOnHeapMemory = maxOnHeapMemory;
            this.maxOffHeapMemory = maxOffHeapMemory;
            this.cleanupThreshold = cleanupThreshold;
            this.cleaner = cleaner;
        }

        public MemoryTracker onHeap(Pool pool, PoolCleaner<?> cleaner)
        {
            return new MemoryTracker(maxOnHeapMemory, cleanupThreshold, cleaner);
        }

        public MemoryTracker offHeap(Pool pool, PoolCleaner<?> cleaner)
        {
            return new MemoryTracker(maxOffHeapMemory, cleanupThreshold, cleaner);
        }

        public PoolCleaner<?> cleaner(Pool pool)
        {
            if (cleaner == null)
                return null;
            return new PoolCleaner<>(pool, cleaner);
        }
    }
}
