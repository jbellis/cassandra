package org.apache.cassandra.utils.memory;

import org.apache.cassandra.utils.concurrent.WaitQueue;

import java.util.concurrent.atomic.AtomicLongFieldUpdater;


/**
 * Represents an amount of memory used for a given purpose, that can be allocated to specific tasks through
 * child MemoryOwner objects. MemoryOwner and MemoryTracker correspond approximately to PoolAllocator and Pool,
 * respectively, with the MemoryTracker bookkeeping the total shared use of resources, and the MemoryOwner the amount
 * checked out and in use by a specific PoolAllocator.
 *
 * Note the difference between acquire() and allocate(); allocate() makes more resources available to all owners,
 * and acquire() makes shared resources unavailable but still recorded. An Owner must always acquire resources,
 * but only needs to allocate if there are none already available. This distinction is not always meaningful.
 */
public class MemoryTracker
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
    private final PoolCleanerThread<?> cleaner;

    public MemoryTracker(long limit, float cleanThreshold, PoolCleanerThread<?> cleaner)
    {
        this.limit = limit;
        this.cleanThreshold = cleanThreshold;
        this.cleaner = cleaner;
        updateNextClean();
    }

    /** Methods for tracking and triggering a clean **/

    boolean needsCleaning()
    {
        return used() >= nextClean && updateNextClean();
    }

    void maybeClean()
    {
        if (needsCleaning() && cleaner != null)
            cleaner.trigger();
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
                return true;
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
                return;
        }
    }

    // 'acquires' an amount of memory, and maybe also marks it allocated. This method is meant to be overridden
    // by implementations with a separate concept of acquired/allocated. As this method stands, an acquire
    // without an allocate is a no-op (acquisition is achieved through allocation), however a release (where size < 0)
    // is always processed and accounted for in allocated.
    void adjustAcquired(long size, boolean alsoAllocated)
    {
        if (size > 0 || alsoAllocated)
        {
            if (alsoAllocated)
                adjustAllocated(size);
            maybeClean();
        }
        else if (size < 0)
        {
            adjustAllocated(size);
            hasRoom.signalAll();
        }
    }

    // space reclaimed should be released prior to calling this, to avoid triggering unnecessary cleans
    void adjustReclaiming(long reclaiming)
    {
        if (reclaiming == 0)
            return;
        reclaimingUpdater.addAndGet(this, reclaiming);
        if (reclaiming < 0 && updateNextClean() && cleaner != null)
            cleaner.trigger();
    }

    public boolean isExceeded()
    {
        return allocated > limit;
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

    private static final AtomicLongFieldUpdater<MemoryTracker> allocatedUpdater = AtomicLongFieldUpdater.newUpdater(MemoryTracker.class, "allocated");
    private static final AtomicLongFieldUpdater<MemoryTracker> reclaimingUpdater = AtomicLongFieldUpdater.newUpdater(MemoryTracker.class, "reclaiming");

    public MemoryOwner newOwner()
    {
        return new MemoryOwner(this);
    }
}

