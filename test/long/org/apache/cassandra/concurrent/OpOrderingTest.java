package org.apache.cassandra.concurrent;

import org.apache.cassandra.utils.concurrent.OpOrdering;
import org.cliffc.high_scale_lib.NonBlockingHashMap;
import org.junit.*;
import org.slf4j.*;

import static org.junit.Assert.*;

import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class OpOrderingTest
{

    private static final Logger logger = LoggerFactory.getLogger(NonBlockingQueueTest.class);

    static final int CONSUMERS = 4;
    static final int PRODUCERS = 32;

    static final long RUNTIME = TimeUnit.MINUTES.toMillis(5);
    static final long REPORT_INTERVAL = TimeUnit.MINUTES.toMillis(1);

    static final Thread.UncaughtExceptionHandler handler = new Thread.UncaughtExceptionHandler()
    {
        @Override
        public void uncaughtException(Thread t, Throwable e)
        {
            System.err.println(t.getName() + ": " + e.getMessage());
            e.printStackTrace();
        }
    };

    final OpOrdering ordering = new OpOrdering();
    final AtomicInteger errors = new AtomicInteger();

    class TestOrdering implements Runnable
    {

        final int[] waitNanos = new int[1 << 16];
        volatile State state = new State();
        final ScheduledExecutorService sched;

        TestOrdering(ExecutorService exec, ScheduledExecutorService sched)
        {
            this.sched = sched;
            final ThreadLocalRandom rnd = ThreadLocalRandom.current();
            for (int i = 0 ; i < waitNanos.length ; i++)
                waitNanos[i] = rnd.nextInt(5000);
            for (int i = 0 ; i < PRODUCERS / CONSUMERS ; i++)
                exec.execute(new Producer());
            exec.execute(this);
        }

        @Override
        public void run()
        {
            final long until = System.currentTimeMillis() + RUNTIME;
            long lastReport = System.currentTimeMillis();
            long count = 0;
            long opCount = 0;
            while (true)
            {
                long now = System.currentTimeMillis();
                if (now > until)
                    break;
                if (now > lastReport + REPORT_INTERVAL)
                {
                    lastReport = now;
                    logger.info(String.format("%s: Executed %d barriers with %d operations. %.0f%% complete.",
                            Thread.currentThread().getName(), count, opCount, 100 * (1 - ((until - now) / (double) RUNTIME))));
                }
                try
                {
                    Thread.sleep(0, waitNanos[((int) (count & (waitNanos.length - 1)))]);
                } catch (InterruptedException e)
                {
                    e.printStackTrace();
                }

                final State s = state;
                s.barrier = ordering.newBarrier();
                s.replacement = new State();
                s.barrier.issue();
                s.barrier.await();
                s.check();
                opCount += s.totalCount();
                state = s.replacement;
                sched.schedule(new Runnable()
                {
                    @Override
                    public void run()
                    {
                        s.check();
                    }
                }, 1, TimeUnit.SECONDS);
                count++;
            }
        }

        class State
        {

            volatile OpOrdering.Barrier barrier;
            volatile State replacement;
            final NonBlockingHashMap<OpOrdering.Ordered, AtomicInteger> count = new NonBlockingHashMap<>();
            int checkCount = -1;

            boolean accept(OpOrdering.Ordered ordered)
            {
                if (barrier != null && !barrier.accept(ordered))
                    return false;
                AtomicInteger c;
                if (null == (c = count.get(ordered)))
                {
                    count.putIfAbsent(ordered, new AtomicInteger());
                    c = count.get(ordered);
                }
                c.incrementAndGet();
                return true;
            }

            int totalCount()
            {
                int c = 0;
                for (AtomicInteger v : count.values())
                    c += v.intValue();
                return c;
            }

            void check()
            {
                boolean delete;
                if (checkCount >= 0)
                {
                    if (checkCount != totalCount())
                    {
                        errors.incrementAndGet();
                        logger.error("Received size changed after barrier finished: {} vs {}", checkCount, totalCount());
                    }
                    delete = true;
                }
                else
                {
                    checkCount = totalCount();
                    delete = false;
                }
                for (Map.Entry<OpOrdering.Ordered, AtomicInteger> e : count.entrySet())
                {
                    if (e.getKey().compareTo(barrier.getSyncPoint()) > 0)
                    {
                        errors.incrementAndGet();
                        logger.error("Received an operation that was created after the barrier was issued.");
                    }
                    if (TestOrdering.this.count.get(e.getKey()).intValue() != e.getValue().intValue())
                    {
                        errors.incrementAndGet();
                        logger.error("Missing registered operations. {} vs {}", TestOrdering.this.count.get(e.getKey()).intValue(), e.getValue().intValue());
                    }
                    if (delete)
                        TestOrdering.this.count.remove(e.getKey());
                }
            }

        }

        final NonBlockingHashMap<OpOrdering.Ordered, AtomicInteger> count = new NonBlockingHashMap<>();

        class Producer implements Runnable
        {
            public void run()
            {
                while (true)
                {
                    AtomicInteger c;
                    OpOrdering.Ordered ordered = ordering.start();
                    try
                    {
                        if (null == (c = count.get(ordered)))
                        {
                            count.putIfAbsent(ordered, new AtomicInteger());
                            c = count.get(ordered);
                        }
                        c.incrementAndGet();
                        State s = state;
                        while (!s.accept(ordered))
                            s = s.replacement;
                    }
                    finally
                    {
                        ordered.finishOne();
                    }
                }
            }
        }

    }

    @Test
    public void testOrdering() throws InterruptedException
    {
        errors.set(0);
        Thread.setDefaultUncaughtExceptionHandler(handler);
        final ExecutorService exec = Executors.newCachedThreadPool(new NamedThreadFactory("checker"));
        final ScheduledExecutorService checker = Executors.newScheduledThreadPool(1, new NamedThreadFactory("checker"));
        for (int i = 0 ; i < CONSUMERS ; i++)
            new TestOrdering(exec, checker);
        exec.shutdown();
        exec.awaitTermination((long) (RUNTIME * 1.1), TimeUnit.MILLISECONDS);
        assertTrue(exec.isShutdown());
        assertTrue(errors.get() == 0);
    }


}
