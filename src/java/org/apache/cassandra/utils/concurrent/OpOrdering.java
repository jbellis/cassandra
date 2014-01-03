package org.apache.cassandra.utils.concurrent;

import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

/**
 * <p>A class for providing synchronization between producers and consumers that do not
 * communicate directly with each other, but where the consumers need to process their
 * work in contiguous batches. In particular this is useful for both CommitLog and Memtable
 * where the producers are modifying a structure that the consumer only batch syncs, but
 * needs to know what 'position' the work is at for co-ordination with other processes,
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
            state.setReplacement(new State())
            state.doSomethingToPrepareForBarrier();

            state.barrier = ordering.newBarrier();
            // issue() MUST be called after newBarrier() else barrier.accept()
            // will always return true, and barrier.await() will fail
            state.barrier.issue();

            // wait for all producer work started prior to the barrier to complete
            state.barrier.await();

            state = state.getReplacement();

            state.doSomethingWithExclusiveAccess();
        }

        public void produce()
        {
            Ordered ordered = ordering.start();
            try
            {
                SharedState s = state;
                while (!s.barrier.accept(ordered))
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
     * Start an operation against this OpOrdering, and return the Ordered instance that manages it.
     * Once the operation is completed Ordered.finishOne() MUST be called EXACTLY once for this operation.
     * @return
     */
    public Ordered start()
    {
        while (true)
        {
            Ordered cur = current;
            if (cur.register())
                return cur;
        }
    }

    /**
     * Start an operation that can be used for a longer running transaction, that periodically reaches points that
     * can be considered to restart the transaction. This is stronger than 'safe', as it declares that all guarded
     * entry points have been exited and will be re-entered, or an equivalent guarantee can be made that no reclaimed
     * resources are being referenced.
     *
     * <pre>
     * ReusableOrdered ord = startSync();
     * while (...)
     * {
     *     ...
     *     ord.sync();
     * }
     * ord.finish();
     *</pre>
     * is semantically equivalent to (but more efficient than):
     *<pre>
     * Ordered ord = start();
     * while (...)
     * {
     *     ...
     *     ord.finishOne();
     *     ord = start();
     * }
     * ord.finishOne();
     * </pre>
     */
    public SyncingOrdered startSync()
    {
        return new SyncingOrdered();
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
         *
         * Two other parallel states are SAFE and ISBLOCKING:
         *
         * safe => all running operations were paused, so safe wrt memory access. Like a java safe point, except
         * that it only provides ordering guarantees, as the safe point can be exited at any point. The only reason
         * we require all to be stopped is to avoid tracking which threads are safe at any point, though we may want
         * to do so in future.
         *
         * isBlocking => a barrier that is waiting on us (either directly, or via a future Ordered) is blocking general
         * progress. This state is entered by calling Barrier.markBlocking(). If the running operations are blocked
         * on a Signal that is also registered with the isBlockingSignal (probably through isSafeBlockingSignal)
         * then they will be notified that they are blocking forward progress, and may take action to avoid that.
         */

        private volatile Ordered prev, next;
        private final long id; // monotonically increasing id for compareTo()
        private volatile int running; // number of operations currently running
        private volatile int safe; // number of operations currently running but 'safe' (see below)
        private volatile boolean isBlocking; // indicates running operations are blocking future barriers
        private final WaitQueue isBlockingSignal = new WaitQueue(); // signal to wait on to indicate isBlocking is true
        private final WaitQueue waiting = new WaitQueue(); // signal to wait on for safe/completion

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
        private void expire()
        {
            while (true)
            {
                int cur = running;
                if (cur < 0)
                    throw new IllegalStateException();
                if (runningUpdater.compareAndSet(this, cur, -1 - cur))
                {
                    // if we're already finished (no running ops), unlink ourselves
                    if (-1 - cur == FINISHED)
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
                int cur = running;
                if (cur < 0)
                    return false;
                if (runningUpdater.compareAndSet(this, cur, cur + 1))
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
                int cur = running;
                if (cur < 0)
                {
                    if (runningUpdater.compareAndSet(this, cur, cur + 1))
                    {
                        if (cur + 1 == FINISHED)
                        {
                            // if we're now finished, unlink ourselves
                            unlink();
                        }
                        else
                        {
                            // otherwise we have modified the result of isSafe(), so maybe signal
                            maybeSignalSafe();
                        }
                        return;
                    }
                }
                else if (runningUpdater.compareAndSet(this, cur, cur - 1))
                {
                    maybeSignalSafe();
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
            end.maybeSignalSafe();
        }

        /**
         * indicates a barrier we are behind is, or maybe, blocking general progress,
         * so we should try more aggressively to progress
         * @return
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
         * internal convenience method indicating if all running operations ON THIS ORDERED ONLY (not preceding ones)
         * are currently 'safe' (generally means waiting on a SafeSignal), i.e. that they are currently guaranteed
         * not to be in the middle of reading memory guarded by this OpOrdering. This is used to prevent blocked
         * operations from preventing off-heap allocator GC progress.
         */

        // TODO: Safe should be a requested operation for each Ordered, that results in each op marking itself as past
        // the requested safe point, thereby not needing to match safe with unsafe, nor requiring all threads to remain
        // in the safe point for the safe point to be considered reached.
        // if possible, should be used through safe(Signal)
        private boolean isSafe()
        {
            int safe = this.safe;
            int running = this.running;
            return (safe == -1 - running) | (safe == running);
        }

        /**
         * indicate a running operation is 'safe' wrt memory accesses, i.e. is waiting or at some other safe point.
         * must be proceeded by a single call to markOneUnsafe()
         */
        public void markOneSafe()
        {
            safeUpdater.incrementAndGet(this);
            maybeSignalSafe();
        }

        private void maybeSignalSafe()
        {
            Ordered cur = this;
            // wake up all waiters on or after us, in case they're interested
            // when we make safe a more general requested check-pointing this can be optimised
            while (cur != null && cur.isSafe())
            {
                cur.waiting.signalAll();
                cur = cur.next;
            }
        }

        /**
         * indicate a running operation that was 'safe' wrt memory accesses, is no longer.
         * if possible, should be used through safe(Signal)
         */
        public void markOneUnsafe()
        {
            safeUpdater.decrementAndGet(this);
        }

        /**
         * wrap the provided signal to mark the thread as safe during any waiting
         */
        public WaitQueue.Signal safe(WaitQueue.Signal signal)
        {
            return new SafeSignal(signal);
        }

        /**
         * wrap the provided signal to mark the thread as safe during any waiting, and to be signalled if the
         * operation gets marked blocking
         */
        public WaitQueue.Signal safeIsBlockingSignal(WaitQueue.Signal signal)
        {
            return new SafeSignal(WaitQueue.any(signal, isBlockingSignal()));
        }

        /**
         * A wrapper class that simply marks safe/unsafe on entry/exit, and delegates to the wrapped signal
         */
        private class SafeSignal implements WaitQueue.Signal
        {

            final WaitQueue.Signal delegate;
            private SafeSignal(WaitQueue.Signal delegate)
            {
                this.delegate = delegate;
            }

            @Override
            public boolean isSignalled()
            {
                return delegate.isSignalled();
            }

            @Override
            public boolean isCancelled()
            {
                return delegate.isCancelled();
            }

            @Override
            public boolean isSet()
            {
                return delegate.isSet();
            }

            @Override
            public boolean checkAndClear()
            {
                return delegate.checkAndClear();
            }

            @Override
            public void cancel()
            {
                delegate.cancel();
            }

            @Override
            public void awaitUninterruptibly()
            {
                markOneSafe();
                try
                {
                    delegate.awaitUninterruptibly();
                }
                finally
                {
                    markOneUnsafe();
                }
            }

            @Override
            public void await() throws InterruptedException
            {
                markOneSafe();
                try
                {
                    delegate.await();
                }
                finally
                {
                    markOneUnsafe();
                }
            }

            @Override
            public long awaitNanos(long nanosTimeout) throws InterruptedException
            {
                markOneSafe();
                try
                {
                    return delegate.awaitNanos(nanosTimeout);
                }
                finally
                {
                    markOneUnsafe();
                }
            }

            @Override
            public boolean awaitUntil(long until) throws InterruptedException
            {
                markOneSafe();
                try
                {
                    return delegate.awaitUntil(until);
                }
                finally
                {
                    markOneUnsafe();
                }
            }
        }

        @Override
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

        static final AtomicIntegerFieldUpdater<Ordered> runningUpdater = AtomicIntegerFieldUpdater.newUpdater(Ordered.class, "running");
        static final AtomicIntegerFieldUpdater<Ordered> safeUpdater = AtomicIntegerFieldUpdater.newUpdater(Ordered.class, "safe");
    }


    /**
     * see {@link #startSync}
     */
    public final class SyncingOrdered
    {

        private Ordered current = start();

        /**
         * Called periodically to indicate we have reached a safe point wrt data guarded by this OpOrdering
         */
        public void sync()
        {
            if (current != OpOrdering.this.current)
            {
                // only swap the operation if we're behind the present
                current.finishOne();
                current = start();
            }
        }

        public Ordered current()
        {
            return current;
        }

        /**
         * Called once our transactions have completed. May safely be called multiple times, with each extra call
         * a no-op.
         */
        public void finish()
        {
            if (current != null)
                current.finishOne();
            current = null;
        }

    }

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

            final Ordered cur;
            synchronized (OpOrdering.this)
            {
                cur = OpOrdering.this.current;
                orderOnOrBefore = cur;
                OpOrdering.this.current = cur.next = new Ordered(cur);
            }
            cur.expire();
        }

        /**
         * Mark all prior operations as blocking, potentially signalling them to more aggressively make progress
         */
        public void markBlocking()
        {
            Ordered cur = orderOnOrBefore;
            while (cur != null)
            {
                cur.isBlocking = true;
                cur.isBlockingSignal.signalAll();
                cur = cur.prev;
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
            return check(false);
        }

        /**
         * @return true if all operations started prior to barrier.issue() have either completed or are marked safe.
         * Note that 'safe' is a transient property, and there is no guarantee that the threads remain safe after this call
         * returns true, only that all threads were safe at this point. like the rest of this classes' functionality,
         * it offers ordering guarantees only.
         */
        public boolean allPriorOpsAreFinishedOrSafe()
        {
            return check(true);
        }

        private boolean check(boolean safe)
        {
            Ordered cur = orderOnOrBefore;
            if (cur == null)
                throw new IllegalStateException("This barrier needs to have issue() called on it before prior operations can complete");
            if (cur.next.prev == null)
                return true;
            while (safe && cur != null)
            {
                safe = cur.isSafe();
                cur = cur.prev;
            }
            return safe;
        }

        /**
         * wait for all operations started prior to issuing the barrier to complete
         */
        public void await()
        {
            await(false);
            assert orderOnOrBefore.running == FINISHED;
        }

        /**
         * wait for allPriorOpsAreFinishedOrSafe() to hold; this is a transient property, and may not hold
         * after this method returns.
         */
        public void awaitSafe()
        {
            await(true);
        }

        private void await(boolean safe)
        {
            while (!check(safe))
            {
                WaitQueue.Signal signal = register();
                if (check(safe))
                {
                    signal.cancel();
                    return;
                }
                else
                    signal.awaitUninterruptibly();
            }
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
