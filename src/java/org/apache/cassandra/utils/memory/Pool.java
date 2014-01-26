package org.apache.cassandra.utils.memory;

import org.apache.cassandra.utils.concurrent.OpOrder;
import org.apache.cassandra.utils.concurrent.WaitQueue;

import java.util.concurrent.atomic.AtomicLongFieldUpdater;


/**
 * Represents an amount of memory used for a given purpose, that can be allocated to specific tasks through
 * child AbstractAllocator objects. AbstractAllocator and MemoryTracker correspond approximately to PoolAllocator and Pool,
 * respectively, with the MemoryTracker bookkeeping the total shared use of resources, and the AbstractAllocator the amount
 * checked out and in use by a specific PoolAllocator.
 *
 * Note the difference between acquire() and allocate(); allocate() makes more resources available to all owners,
 * and acquire() makes shared resources unavailable but still recorded. An Owner must always acquire resources,
 * but only needs to allocate if there are none already available. This distinction is not always meaningful.
 */
public abstract class Pool
{
    // total memory/resource permitted to allocate
    public final long limit;

    // ratio of used to spare (both excluding 'reclaiming') at which to trigger a clean
    public final float cleanThreshold;

    // total bytes allocated and reclaiming
    private volatile long allocated;
    private volatile long reclaiming;

    final WaitQueue hasRoom = new WaitQueue();

    // a cache of the calculation determining at what allocation threshold we should next clean, and the cleaner we trigger
    private volatile long nextClean;
    private final PoolCleanerThread<?> cleanerThread;

    public Pool(long limit, float cleanThreshold, Runnable cleaner)
    {
        this.limit = limit;
        this.cleanThreshold = cleanThreshold;
        updateNextClean();
        cleanerThread = cleaner == null ? null : new PoolCleanerThread<>(this, cleaner);
        if (cleanerThread != null)
            cleanerThread.start();
    }

    /** Methods for tracking and triggering a clean **/

    boolean needsCleaning()
    {
        return used() >= nextClean && updateNextClean() && cleanerThread != null;
    }

    void maybeClean()
    {
        if (needsCleaning())
            cleanerThread.trigger();
    }

    private boolean updateNextClean()
    {
        long reclaiming = this.reclaiming;
        return used() >= (nextClean = reclaiming
                + (long) (this.limit * cleanThreshold));
    }

    /** Methods to allocate space **/

    boolean tryAllocate(int size)
    {
        while (true)
        {
            long cur;
            if ((cur = allocated) + size > limit)
                return false;
            if (allocatedUpdater.compareAndSet(this, cur, cur + size))
            {
                maybeClean();
                return true;
            }
        }
    }

    /**
     * apply the size adjustment to allocated, bypassing any limits or constraints. If this reduces the
     * allocated total, we will signal waiters
     */
    void adjustAllocated(long size)
    {
        if (size == 0)
            return;
        while (true)
        {
            long cur = allocated;
            if (allocatedUpdater.compareAndSet(this, cur, cur + size))
            {
                if (size > 0)
                {
                    maybeClean();
                }
                return;
            }
        }
    }

    void release(long size)
    {
        adjustAllocated(-size);
        hasRoom.signalAll();
    }

    // space reclaimed should be released prior to calling this, to avoid triggering unnecessary cleans
    void adjustReclaiming(long reclaiming)
    {
        if (reclaiming == 0)
            return;
        reclaimingUpdater.addAndGet(this, reclaiming);
        if (reclaiming < 0 && updateNextClean() && cleanerThread != null)
            cleanerThread.trigger();
    }

    public long allocated()
    {
        return allocated;
    }

    public long used()
    {
        return allocated;
    }

    public long reclaiming()
    {
        return reclaiming;
    }

    private static final AtomicLongFieldUpdater<Pool> allocatedUpdater = AtomicLongFieldUpdater.newUpdater(Pool.class, "allocated");
    private static final AtomicLongFieldUpdater<Pool> reclaimingUpdater = AtomicLongFieldUpdater.newUpdater(Pool.class, "reclaiming");

    public abstract PoolAllocator newAllocator(OpOrder writes);
}

