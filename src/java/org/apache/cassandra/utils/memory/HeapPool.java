package org.apache.cassandra.utils.memory;

import org.apache.cassandra.utils.concurrent.OpOrdering;

public class HeapPool extends Pool
{
    public HeapPool(long maxOnHeapMemory, long maxOffHeapMemory, float cleanupThreshold, Runnable cleaner)
    {
        super(new Setup(maxOnHeapMemory, maxOffHeapMemory, cleanupThreshold, cleaner));
    }

    @Override
    public HeapPoolAllocator newAllocator(OpOrdering writes)
    {
        return new HeapPoolAllocator(this);
    }
}
