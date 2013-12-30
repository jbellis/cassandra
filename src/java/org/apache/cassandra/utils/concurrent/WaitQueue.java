package org.apache.cassandra.utils.concurrent;

import com.yammer.metrics.core.TimerContext;
import org.slf4j.*;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.locks.LockSupport;

/**
 * <p>A relatively easy to use utility for general purpose thread signalling.</p>
 * <p>Usage on a thread awaiting a state change using a WaitQueue q is:</p>
 * <pre>
 * {@code
 *      while (!conditionMet())
 *          WaitSignal s = q.register();
 *              if (!conditionMet())    // or, perhaps more correctly, !conditionChanged()
 *                  s.await();
 *              else
 *                  s.cancel();
 * }
 * </pre>
 * A signalling thread, AFTER changing the state, then calls q.signal() to wake up one, or q.signalAll()
 * to wake up all, waiting threads.
 * <p>To understand intuitively how this class works, the idea is simply that a thread, once it considers itself
 * incapable of making progress, registers to be awoken once that changes. Since this could have changed between
 * checking and registering (in which case the thread that made this change would have been unable to signal it),
 * it checks the condition again, sleeping only if it hasn't changed/still is not met.</p>
 * <p>This thread synchronisation scheme has some advantages over Condition objects and Object.wait/notify in that no monitor
 * acquisition is necessary and, in fact, besides the actual waiting on a signal, all operations are non-blocking.
 * As a result consumers can never block producers, nor each other, or vice versa, from making progress.
 * Threads that are signalled are also put into a RUNNABLE state almost simultaneously, so they can all immediately make
 * progress without having to serially acquire the monitor/lock, reducing scheduler delay incurred.</p>
 *
 * <p>A few notes on utilisation:</p>
 * <p>1. A thread will only exit await() when it has been signalled, but this does not guarantee the condition has not
 * been altered since it was signalled, and depending on your design it is likely the outer condition will need to be
 * checked in a loop, though this is not always the case.</p>
 * <p>2. Each signal is single use, so must be re-registered after each await(). This is true even if it times out.</p>
 * <p>3. If you choose not to wait on the signal (because the condition has been met before you waited on it)
 * you must cancel() the signal if the signalling thread uses signal() to awake waiters; otherwise signals will be
 * lost. If signalAll() is used but infrequent, and register() is frequent, cancel() should still be used to prevent the
 * queue growing unboundedly. Similarly, if you provide a TimerContext, cancel should be used to ensure it is not erroneously
 * counted towards wait time.</p>
 * <p>4. Care must be taken when selecting conditionMet() to ensure we are waiting on the condition that actually
 * indicates progress is possible. In some complex cases it may be tempting to wait on a condition that is only indicative
 * of local progress, not progress on the task we are aiming to complete, and a race may leave us waiting for a condition
 * to be met that we no longer need.
 * <p>5. This scheme is not fair</p>
 * <p>6. Only the thread that calls register() may call await()</p>
 */
public final class WaitQueue
{

    private static final Logger logger = LoggerFactory.getLogger(WaitQueue.class);

    private static final int CANCELLED = -1;
    private static final int SIGNALLED = 1;
    private static final int NOT_SET = 0;

    public static interface Signal
    {

        public boolean isSignalled();
        public boolean isCancelled();
        public boolean isSet();
        public boolean checkAndClear();

        /**
         * Should only be called by the registered thread. Indicates the signal can be retired,
         * and if signalled propagates the signal to another waiting thread
         */
        public abstract void cancel();
        public void awaitUninterruptibly();
        public void await() throws InterruptedException;
        public long awaitNanos(long nanosTimeout) throws InterruptedException;
        public boolean awaitUntil(long until) throws InterruptedException;
    }

    public static abstract class AbstractSignal implements Signal
    {

        public void awaitUninterruptibly()
        {
            boolean interrupted = false;
            while (!isSignalled())
            {
                if (Thread.currentThread().interrupted())
                    interrupted = true;
                LockSupport.park();
            }
            if (interrupted)
                Thread.currentThread().interrupt();
            checkAndClear();
        }

        public void await() throws InterruptedException
        {
            while (!isSignalled())
            {
                checkInterrupted();
                LockSupport.park();
            }
            checkAndClear();
        }

        public long awaitNanos(long nanosTimeout) throws InterruptedException
        {
            long start = System.nanoTime();
            while (!isSignalled())
            {
                checkInterrupted();
                LockSupport.parkNanos(nanosTimeout);
            }
            checkAndClear();
            return nanosTimeout - (System.nanoTime() - start);
        }

        public boolean awaitUntil(long until) throws InterruptedException
        {
            while (until < System.currentTimeMillis() && !isSignalled())
            {
                checkInterrupted();
                LockSupport.parkUntil(until);
            }
            return checkAndClear();
        }

        private void checkInterrupted() throws InterruptedException
        {
            if (Thread.interrupted())
            {
                cancel();
                throw new InterruptedException();
            }
        }

    }

    final class TimedSignal extends RegisteredSignal
    {
        private final TimerContext context;

        private TimedSignal(TimerContext context)
        {
            this.context = context;
        }

        @Override
        public boolean checkAndClear()
        {
            context.stop();
            return super.checkAndClear();
        }

        @Override
        public void cancel()
        {
            if (!isCancelled())
            {
                context.stop();
                super.cancel();
            }
        }

    }

    private class RegisteredSignal extends AbstractSignal
    {

        private volatile Thread thread = Thread.currentThread();
        volatile int state;

        public boolean isSignalled()
        {
            return state == SIGNALLED;
        }

        public boolean isCancelled()
        {
            return state == CANCELLED;
        }

        public boolean isSet()
        {
            return state != NOT_SET;
        }

        private boolean signal()
        {
            if (!isSet() && signalledUpdater.compareAndSet(this, NOT_SET, SIGNALLED))
            {
                LockSupport.unpark(thread);
                thread = null;
                return true;
            }
            return false;
        }

        public boolean checkAndClear()
        {
            if (!isSet() && signalledUpdater.compareAndSet(this, NOT_SET, CANCELLED))
            {
                thread = null;
                cleanUpCancelled();
                return false;
            }
            // must now be signalled assuming correct API usage
            return true;
        }

        /**
         * Should only be called by the registered thread. Indicates the signal can be retired,
         * and if signalled propagates the signal to another waiting thread
         */
        public void cancel()
        {
            if (isCancelled())
                return;
            if (!signalledUpdater.compareAndSet(this, NOT_SET, CANCELLED))
            {
                // must already be signalled - switch to cancelled and
                state = CANCELLED;
                // propagate the signal
                WaitQueue.this.signal();
            }
            thread = null;
            cleanUpCancelled();
        }

    }

    private static final AtomicIntegerFieldUpdater signalledUpdater = AtomicIntegerFieldUpdater.newUpdater(RegisteredSignal.class, "state");

    // the waiting signals
    private final NonBlockingQueue<RegisteredSignal> queue = new NonBlockingQueue<>();

    /**
     * The calling thread MUST be the thread that uses the signal
     * @return
     */
    public Signal register()
    {
        RegisteredSignal signal = new RegisteredSignal();
        queue.append(signal);
        return signal;
    }

    /**
     * The calling thread MUST be the thread that uses the signal.
     * If the Signal is waited on, context.stop() will be called when the wait times out, the Signal is signalled,
     * or the waiting thread is interrupted.
     * @return
     */
    public Signal register(TimerContext context)
    {
        assert context != null;
        RegisteredSignal signal = new TimedSignal(context);
        queue.append(signal);
        return signal;
    }

    /**
     * Signal one waiting thread
     */
    public boolean signal()
    {
        if (!hasWaiters())
            return false;
        while (true)
        {
            RegisteredSignal s = queue.poll();
            if (s == null || s.signal())
                return s != null;
        }
    }

    /**
     * Signal all waiting threads
     */
    public void signalAll()
    {
        if (!hasWaiters())
            return;
        List<Thread> woke = null;
        if (logger.isTraceEnabled())
            woke = new ArrayList<>();
        long start = System.nanoTime();
        // we wake up only a snapshot of the queue, to avoid a race where the condition is not met and the woken thread
        // immediately waits on the queue again
        for (RegisteredSignal s : queue.snap())
        {
            if (s.signal() && woke != null)
                woke.add(s.thread);
            // move the queue forwards
            queue.advanceHeadIf(s);
        }
        long end = System.nanoTime();
        if (woke != null)
            logger.trace("Woke up {} in {}ms from {}", woke, (end - start) * 0.000001d, Thread.currentThread().getStackTrace()[2]);
    }

    private void cleanUpCancelled()
    {
        // attempt to remove the cancelled from the beginning only;
        boolean cleaned = false;
        while (true)
        {
            RegisteredSignal s = queue.peek();
            if (s == null || !s.isCancelled())
                break;
            cleaned = true;
            queue.advanceHeadIf(s);
        }
        // if we succeed in doing so, assume we've removed all cancelled items; since we're only doing this to avoid
        // the queue growing unboundedly, this is safe, as otherwise we must cancel again after cleaning the front
        if (cleaned)
            return;
        // otherwise, attempt 'unsafe' (maybe unsuccessful) removal from the rest of the queue; since we will perform
        // this repeatedly we should tend towards a small/empty set of cancelled items remaining on the queue, even
        // if some interleaved deletes are lost
        Iterator<RegisteredSignal> iter = queue.iterator();
        while (iter.hasNext())
        {
            RegisteredSignal s = iter.next();
            if (s.isCancelled())
                iter.remove();
        }
    }

    public boolean hasWaiters()
    {
        return !queue.isEmpty();
    }

    /**
     * Return how many threads are waiting
     * @return
     */
    public int getWaiting()
    {
        if (queue.isEmpty())
            return 0;
        Iterator<RegisteredSignal> iter = queue.iterator();
        int count = 0;
        while (iter.hasNext())
        {
            Signal next = iter.next();
            if (!next.isCancelled())
                count++;
        }
        return count;
    }

    public static Signal any(Signal ... signals)
    {
        return new AnySignal(signals);
    }

    public static Signal all(Signal ... signals)
    {
        return new AllSignal(signals);
    }

    private abstract static class MultiSignal extends AbstractSignal
    {

        final Signal[] signals;
        protected MultiSignal(Signal[] signals)
        {
            this.signals = signals;
        }

        @Override
        public boolean isCancelled()
        {
            for (Signal signal : signals)
                if (!signal.isCancelled())
                    return false;
            return true;
        }

        @Override
        public boolean checkAndClear()
        {
            for (Signal signal : signals)
                signal.checkAndClear();
            return isSignalled();
        }

        @Override
        public void cancel()
        {
            for (Signal signal : signals)
                signal.cancel();
        }

    }

    private static class AnySignal extends MultiSignal
    {

        protected AnySignal(Signal ... signals)
        {
            super(signals);
        }

        @Override
        public boolean isSignalled()
        {
            for (Signal signal : signals)
                if (signal.isSignalled())
                    return true;
            return false;
        }

        @Override
        public boolean isSet()
        {
            for (Signal signal : signals)
                if (signal.isSet())
                    return true;
            return false;
        }
    }

    private static class AllSignal extends MultiSignal
    {

        protected AllSignal(Signal ... signals)
        {
            super(signals);
        }

        @Override
        public boolean isSignalled()
        {
            for (Signal signal : signals)
                if (!signal.isSignalled())
                    return false;
            return true;
        }

        @Override
        public boolean isSet()
        {
            for (Signal signal : signals)
                if (!signal.isSet())
                    return false;
            return true;
        }

    }

}
