package org.apache.cassandra.utils.memory;

import org.apache.cassandra.utils.concurrent.OpOrder;

public class HeapPool extends Pool
{
    public HeapPool(long maxOnHeapMemory, long maxOffHeapMemory, float cleanupThreshold, Runnable cleaner)
    {
        super(maxOnHeapMemory, maxOffHeapMemory, cleanupThreshold, cleaner);
    }

    @Override
    public HeapPoolAllocator newAllocator(OpOrder writes)
    {
        return new HeapPoolAllocator(this);
    }
}
