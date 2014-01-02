package org.apache.cassandra.utils.memory;

import org.apache.cassandra.utils.concurrent.WaitQueue;

import java.util.concurrent.atomic.AtomicLongFieldUpdater;

// note difference between acquire() and allocate()
public class MemoryTracker
{

    // total memory/resource permitted to allocate
    public final long limit;

    // ratio of used to spare (both excluding 'reclaiming') at which to trigger a clean
    public final float cleanThreshold;

    // total bytes allocated and reclaiming
    private volatile long allocated;
    private volatile long acquired;
    private volatile long reclaiming;

    final WaitQueue hasRoom = new WaitQueue();

    // a cache of the calculation determining at what allocation threshold we should next clean, and the cleaner we trigger
    private volatile long nextClean;
    private final PoolCleaner<?> cleaner;

    public MemoryTracker(long limit, float cleanThreshold, PoolCleaner<?> cleaner)
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
        return acquired >= (nextClean = reclaiming
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

    // when a pool caches an allocated quantity for reuse this method is used to track the use of
    // memory that is in use / owned.
    void acquired(long size)
    {
        acquiredUpdater.addAndGet(this, size);
        maybeClean();
    }

    // un-acquires (as opposed to deallocates) the amount of memory, and signals any waiting threads
    void release(long size)
    {
        acquiredUpdater.addAndGet(this, -size);
        hasRoom.signalAll();
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
        return acquired;
    }

    public long reclaiming()
    {
        return reclaiming;
    }

    private static final AtomicLongFieldUpdater<MemoryTracker> allocatedUpdater = AtomicLongFieldUpdater.newUpdater(MemoryTracker.class, "allocated");
    private static final AtomicLongFieldUpdater<MemoryTracker> acquiredUpdater = AtomicLongFieldUpdater.newUpdater(MemoryTracker.class, "acquired");
    private static final AtomicLongFieldUpdater<MemoryTracker> reclaimingUpdater = AtomicLongFieldUpdater.newUpdater(MemoryTracker.class, "reclaiming");

    public MemoryOwner newOwner()
    {
        return new MemoryOwner(this);
    }

}

