package org.apache.cassandra.utils.memory;

import org.apache.cassandra.concurrent.NamedThreadFactory;
import org.apache.cassandra.utils.concurrent.OpOrder;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public abstract class Pool
{
    final PoolCleaner<?> cleaner;

    // the total memory used by this pool
    public final MemoryTracker onHeap;
    public final MemoryTracker offHeap;

    Pool(long maxOnHeapMemory, long maxOffHeapMemory, float cleanupThreshold, Runnable cleaner)
    {
        this.cleaner = new PoolCleaner<>(this, cleaner);
        this.onHeap = new MemoryTracker(maxOnHeapMemory, cleanupThreshold, this.cleaner);
        this.offHeap = new MemoryTracker(maxOffHeapMemory, cleanupThreshold, this.cleaner);
        // start a thread to run the cleaner
        new NamedThreadFactory(this.getClass().getSimpleName() + "Cleaner").newThread(this.cleaner).start();
    }

    public abstract PoolAllocator newAllocator(OpOrder writes);
}
