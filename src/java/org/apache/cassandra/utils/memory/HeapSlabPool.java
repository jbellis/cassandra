package org.apache.cassandra.utils.memory;

import org.apache.cassandra.utils.concurrent.OpOrder;

public class HeapSlabPool extends Pool
{
    public HeapSlabPool(long maxOnHeapMemory, long maxOffHeapMemory, float cleanupThreshold, Runnable cleaner)
    {
        super(maxOnHeapMemory, maxOffHeapMemory, cleanupThreshold, cleaner);
    }

    public HeapSlabAllocator newAllocator(OpOrder writes)
    {
        return new HeapSlabAllocator(this);
    }
}
