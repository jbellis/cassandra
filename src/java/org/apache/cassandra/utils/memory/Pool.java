package org.apache.cassandra.utils.memory;

import org.apache.cassandra.utils.concurrent.OpOrder;

public abstract class Pool
{
    final PoolCleanerThread<?> cleanerThread;

    // the total memory used by this pool
    public final MemoryTracker onHeap;
    public final MemoryTracker offHeap;

    Pool(long maxOnHeapMemory, long maxOffHeapMemory, float cleanupThreshold, Runnable cleaner)
    {
        cleanerThread = new PoolCleanerThread<>(this, cleaner);
        onHeap = new MemoryTracker(maxOnHeapMemory, cleanupThreshold, cleanerThread);
        offHeap = new MemoryTracker(maxOffHeapMemory, cleanupThreshold, cleanerThread);
        cleanerThread.start();
    }

    public abstract PoolAllocator newAllocator(OpOrder writes);
}
