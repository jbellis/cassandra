package org.apache.cassandra.utils.concurrent;

import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

/**
 * <p>A class for providing synchronization between producers and consumers that do not
 * communicate directly with each other, but where the consumers need to process their
 * work in contiguous batches. In particular this is useful for both CommitLog and Memtable
 * where the producers (writing threads) are modifying a structure that the consumer
 * (flush executor) only batch syncs, but needs to know what 'position' the work is at
 * for co-ordination with other processes,
 *
 * <p>The typical usage is something like:
 * <pre>
     public final class ExampleShared
     {
        final OpOrdering ordering = new OpOrdering();
        volatile SharedState state;

        static class SharedState
        {
            volatile Barrier barrier;

            // ...
        }

        public void consume()
        {
            SharedState state = this.state;
            state.setReplacement(new State())
            state.doSomethingToPrepareForBarrier();

            state.barrier = ordering.newBarrier();
            // issue() MUST be called after newBarrier() else barrier.accept()
            // will always return true, and barrier.await() will fail
            state.barrier.issue();

            // wait for all producer work started prior to the barrier to complete
            state.barrier.await();

            // change the shared state to its replacement, as the current state will no longer be used by producers
            this.state = state.getReplacement();

            state.doSomethingWithExclusiveAccess();
        }

        public void produce()
        {
            Ordered ordered = ordering.start();
            try
            {
                SharedState s = state;
                while (s.barrier != null && !s.barrier.accept(ordered))
                    s = s.getReplacement();
                s.doProduceWork();
            }
            finally
            {
                ordered.finishOne();
            }
        }
    }
 * </pre>
 */
public class OpOrdering
{
    /**
     * Constant that when an Ordered.running is equal to, indicates the Ordered is complete
     */
    private static final int FINISHED = -1;

    /**
     * A linked list starting with the most recent Ordered object, i.e. the one we should start new operations from,
     * with (prev) links to any incomplete Ordered instances, and (next) links to any potential future Ordered instances.
     * Once all operations started against an Ordered instance and its ancestors have been finished the next instance
     * will unlink this one
     */
    private volatile Ordered current = new Ordered();

    /**
     * Start an operation against this OpOrdering.
     * Once the operation is completed Ordered.finishOne() MUST be called EXACTLY once for this operation.
     *
     * @return the Ordered instance that manages this OpOrdering
     */
    public Ordered start()
    {
        while (true)
        {
            Ordered current = this.current;
            if (current.register())
                return current;
        }
    }

    /**
     * Creates a new barrier. The barrier is only a placeholder until barrier.issue() is called on it,
     * after which all new operations will start against a new Ordered instance that will not be accepted
     * by barrier.accept(), and barrier.await() will return only once all operations started prior to the issue
     * have completed.
     *
     * @return
     */
    public Barrier newBarrier()
    {
        return new Barrier();
    }

    public Ordered getCurrent()
    {
        return current;
    }

    /**
     * Represents a 'batch' of identically ordered operations, i.e. all operations started in the interval between
     * two barrier issuances. For each register() call this is returned, finishOne() must be called exactly once.
     * It should be treated like taking a lock().
     */
    public static final class Ordered implements Comparable<Ordered>
    {
        /**
         * In general this class goes through the following stages:
         * 1) LIVE:      many calls to register() and finishOne()
         * 2) FINISHING: a call to expire() (after a barrier issue), means calls to register() will now fail,
         *               and we are now 'in the past' (new operations will be started against a new Ordered)
         * 3) FINISHED:  once the last finishOne() is called, this Ordered is done. We call unlink().
         * 4) ZOMBIE:    all our operations are finished, but some operations against an earlier Ordered are still
         *               running, or tidying up, so unlink() fails to remove us
         * 5) COMPLETE:  all operations started on or before us are FINISHED (and COMPLETE), so we are unlinked
         * <p/>
         * Another parallel states is ISBLOCKING:
         * <p/>
         * isBlocking => a barrier that is waiting on us (either directly, or via a future Ordered) is blocking general
         * progress. This state is entered by calling Barrier.markBlocking(). If the running operations are blocked
         * on a Signal that is also registered with the isBlockingSignal (probably through isSafeBlockingSignal)
         * then they will be notified that they are blocking forward progress, and may take action to avoid that.
         */

        private volatile Ordered prev, next;
        private final long id; // monotonically increasing id for compareTo()
        private volatile int running = 0; // number of operations currently running.  < 0 means we're expired, and the count of tasks still running is -(running + 1)
        private volatile boolean isBlocking; // indicates running operations are blocking future barriers
        private final WaitQueue isBlockingSignal = new WaitQueue(); // signal to wait on to indicate isBlocking is true
        private final WaitQueue waiting = new WaitQueue(); // signal to wait on for completion

        static final AtomicIntegerFieldUpdater<Ordered> runningUpdater = AtomicIntegerFieldUpdater.newUpdater(Ordered.class, "running");

        // constructs first instance only
        private Ordered()
        {
            this.id = 0;
        }

        private Ordered(Ordered prev)
        {
            this.id = prev.id + 1;
            this.prev = prev;
        }

        // prevents any further operations starting against this Ordered instance
        // if there are no running operations, calls unlink; otherwise, we let the last op to finishOne call it.
        // this means issue() won't have to block for ops to finish.
        private void expire()
        {
            while (true)
            {
                int current = running;
                if (current < 0)
                    throw new IllegalStateException();
                if (runningUpdater.compareAndSet(this, current, -1 - current))
                {
                    // if we're already finished (no running ops), unlink ourselves
                    if (current == 0)
                        unlink();
                    return;
                }
            }
        }

        // attempts to start an operation against this Ordered instance, and returns true if successful.
        private boolean register()
        {
            while (true)
            {
                int current = running;
                if (current < 0)
                    return false;
                if (runningUpdater.compareAndSet(this, current, current + 1))
                    return true;
            }
        }

        /**
         * To be called exactly once for each register() call this object is returned for, indicating the operation
         * is complete
         */
        public void finishOne()
        {
            while (true)
            {
                int current = running;
                if (current < 0)
                {
                    if (runningUpdater.compareAndSet(this, current, current + 1))
                    {
                        if (current + 1 == FINISHED)
                        {
                            // if we're now finished, unlink ourselves
                            unlink();
                        }
                        return;
                    }
                }
                else if (runningUpdater.compareAndSet(this, current, current - 1))
                {
                    return;
                }
            }
        }

        /**
         * called once we know all operations started against this Ordered have completed,
         * however we do not know if operations against its ancestors have completed, or
         * if its descendants have completed ahead of it, so we attempt to create the longest
         * chain from the oldest still linked Ordered. If we can't reach the oldest through
         * an unbroken chain of completed Ordered, we abort, and leave the still completing
         * ancestor to tidy up.
         */
        private void unlink()
        {
            // walk back in time to find the start of the list
            Ordered start = this;
            while (true)
            {
                Ordered prev = start.prev;
                if (prev == null)
                    break;
                // if we haven't finished this Ordered yet abort and let it clean up when it's done
                if (prev.running != FINISHED)
                    return;
                start = prev;
            }

            // now walk forwards in time, in case we finished up late
            Ordered end = this.next;
            while (end.running == FINISHED)
                end = end.next;

            // now walk from first to last, unlinking the prev pointer and waking up any blocking threads
            while (start != end)
            {
                Ordered next = start.next;
                next.prev = null;
                start.waiting.signalAll();
                start = next;
            }
        }

        /**
         * @return true if a barrier we are behind is, or may be, blocking general progress,
         * so we should try more aggressively to progress
         */
        public boolean isBlocking()
        {
            return isBlocking;
        }

        /**
         * register to be signalled when a barrier waiting on us is, or maybe, blocking general progress,
         * so we should try more aggressively to progress
         */
        public WaitQueue.Signal isBlockingSignal()
        {
            return isBlockingSignal.register();
        }

        /**
         * wrap the provided signal to also be signalled if the operation gets marked blocking
         */
        public WaitQueue.Signal isBlockingSignal(WaitQueue.Signal signal)
        {
            return WaitQueue.any(signal, isBlockingSignal());
        }

        public int compareTo(Ordered that)
        {
            // we deliberately use subtraction, as opposed to Long.compareTo() as we care about ordering
            // not which is the smaller value, so this permits wrapping in the unlikely event we exhaust the long space
            long c = this.id - that.id;
            if (c > 0)
                return 1;
            else if (c < 0)
                return -1;
            else
                return 0;
        }
    }

    /**
     * This class represents a synchronisation point providing ordering guarantees on operations started
     * against the enclosing OpOrdering. When issue() is called upon it (may only happen once per Barrier), the
     * Barrier atomically partitions new operations from those already running, and activates its accept() method
     * which indicates if an operation was started before or after this partition. It offers methods to
     * determine, or block until, all prior operations have finished, and a means to indicate to those operations
     * that they are blocking forward progress. See {@link OpOrdering} for idiomatic usage.
     */
    public final class Barrier
    {
        // this Barrier was issued after all Ordered operations started against orderOnOrBefore
        private volatile Ordered orderOnOrBefore;

        // if the barrier has been exposed to all operations prior to .issue() being called, then
        // accept() will return true only for (and for all) those operations started prior to the issue of the barrier
        public boolean accept(Ordered op)
        {
            if (orderOnOrBefore == null)
                return true;
            // we subtract to permit wrapping round the full range of Long - so we only need to ensure
            // there are never Long.MAX_VALUE * 2 total Ordered objects in existence at any one timem which will
            // take care of itself
            return orderOnOrBefore.id - op.id >= 0;
        }

        /**
         * Issues the barrier; must be called after exposing the barrier to any operations it may affect,
         * but before it is used, so that the accept() method is properly synchronised.
         */
        public void issue()
        {
            if (orderOnOrBefore != null)
                throw new IllegalStateException("Can only call issue() once on each Barrier");

            final Ordered current;
            synchronized (OpOrdering.this)
            {
                current = OpOrdering.this.current;
                orderOnOrBefore = current;
                OpOrdering.this.current = current.next = new Ordered(current);
            }
            current.expire();
        }

        /**
         * Mark all prior operations as blocking, potentially signalling them to more aggressively make progress
         */
        public void markBlocking()
        {
            Ordered current = orderOnOrBefore;
            while (current != null)
            {
                current.isBlocking = true;
                current.isBlockingSignal.signalAll();
                current = current.prev;
            }
        }

        /**
         * Register to be signalled once allPriorOpsAreFinished() or allPriorOpsAreFinishedOrSafe() may return true
         */
        public WaitQueue.Signal register()
        {
            return orderOnOrBefore.waiting.register();
        }

        /**
         * @return true if all operations started prior to barrier.issue() have completed
         */
        public boolean allPriorOpsAreFinished()
        {
            Ordered current = orderOnOrBefore;
            if (current == null)
                throw new IllegalStateException("This barrier needs to have issue() called on it before prior operations can complete");
            if (current.next.prev == null)
                return true;
            return false;
        }

        /**
         * wait for all operations started prior to issuing the barrier to complete
         */
        public void await()
        {
            while (!allPriorOpsAreFinished())
            {
                WaitQueue.Signal signal = register();
                if (allPriorOpsAreFinished())
                {
                    signal.cancel();
                    return;
                }
                else
                    signal.awaitUninterruptibly();
            }
            assert orderOnOrBefore.running == FINISHED;
        }

        /**
         * returns the Ordered object we are waiting on - any Ordered with .compareTo(getSyncPoint()) <= 0
         * must complete before await() returns
         */
        public Ordered getSyncPoint()
        {
            return orderOnOrBefore;
        }

    }
}
