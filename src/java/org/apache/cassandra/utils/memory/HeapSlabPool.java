package org.apache.cassandra.utils.memory;

import org.apache.cassandra.utils.concurrent.OpOrdering;

public class HeapSlabPool extends Pool
{

    public HeapSlabPool(long maxOnHeapMemory, long maxOffHeapMemory, float cleanupThreshold, Runnable cleaner)
    {
        super(new Setup(maxOnHeapMemory, maxOffHeapMemory, cleanupThreshold, cleaner));
    }

    @Override
    public HeapSlabAllocator newAllocator(OpOrdering writes)
    {
        return new HeapSlabAllocator(this);
    }

}
