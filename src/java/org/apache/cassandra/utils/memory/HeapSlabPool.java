package org.apache.cassandra.utils.memory;

import org.apache.cassandra.utils.concurrent.OpOrdering;

public class HeapSlabPool extends Pool
{
    public HeapSlabPool(long maxOnHeapMemory, long maxOffHeapMemory, float cleanupThreshold, Runnable cleaner)
    {
        super(maxOnHeapMemory, maxOffHeapMemory, cleanupThreshold, cleaner);
    }

    public HeapSlabAllocator newAllocator(OpOrdering writes)
    {
        return new HeapSlabAllocator(this);
    }
}
