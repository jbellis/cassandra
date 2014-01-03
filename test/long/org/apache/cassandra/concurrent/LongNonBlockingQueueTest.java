package org.apache.cassandra.concurrent;

import com.google.common.collect.*;
import org.apache.cassandra.utils.concurrent.NonBlockingQueue;
import org.apache.cassandra.utils.concurrent.NonBlockingQueueView;
import org.junit.*;
import org.slf4j.*;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.apache.cassandra.utils.concurrent.NonBlockingQueueView.Snap;

public class LongNonBlockingQueueTest
{

    private static final Logger logger = LoggerFactory.getLogger(LongNonBlockingQueueTest.class);

    final int THREAD_COUNT = 32;
    final int THREAD_MASK = 31;
    final ExecutorService exec = Executors.newFixedThreadPool(THREAD_COUNT, new NamedThreadFactory("Test"));

    // all we care about is that iterator removals don't delete anything other than they target,
    // since it makes no guarantees about removal succeeding. To try and test it thoroughly we will
    // spin deleting until all our intended deletes complete successfully.
    // We also partially test snapshots, iterators and views.
    @Test
    public void testIteratorRemoval() throws ExecutionException, InterruptedException
    {

        final NonBlockingQueue<Integer> queue = new NonBlockingQueue<>();
        final List<Future<Boolean>> success = new ArrayList<>();
        final AtomicInteger totalBatchCount = new AtomicInteger();
        final AtomicLong failedDeletes = new AtomicLong();
        final AtomicInteger deleteAttempts = new AtomicInteger();
        for (int i = 0 ; i < THREAD_COUNT ; i++)
        {
            final int offset = i;
            success.add(exec.submit(new Callable<Boolean>()
            {
                @Override
                public Boolean call()
                {
                    final int batchCount = 200;
                    final int batchSize = 1 << 14;
                    for (int batch = 0 ; batch < batchCount ; batch++)
                    {
                        final int end = offset + (THREAD_COUNT * batchSize);
                        NonBlockingQueueView<Integer> view = queue.view();
                        for (int i = offset ; i < end ; i+= THREAD_COUNT)
                            queue.append(i);

                        // check all my items are still there
                        Iterable<Integer> snap = view.snap();
                        int find = offset;
                        for (Integer v : snap)
                        {
                            if ((v & THREAD_MASK) == offset)
                            {
                                if (v != find)
                                {
                                    logger.error("Unexpected next value (1); expected {}, found {}", find, v);
                                    return Boolean.FALSE;
                                }
                                find += THREAD_COUNT;
                            }
                        }
                        if (find != end)
                        {
                            logger.error("Unexpected last value (1); expected {}, found {}", end, find);
                            return Boolean.FALSE;
                        }

                        // delete every other item, and loop until they're all gone, failing if we cannot delete more than
                        // 50% over 1000 tries
                        int tries = 0;
                        int notmissing = 0;
                        while (++tries <= 1000)
                        {
                            notmissing = 0;
                            find = offset;
                            Iterator<Integer> iter = snap.iterator();
                            while (iter.hasNext() && find != end)
                            {
                                Integer next = iter.next();
                                if ((next & THREAD_MASK) == offset)
                                {
                                    if (next == find)
                                        find += THREAD_COUNT << 1;
                                    else if (next > find)
                                    {
                                        logger.error("Unexpected next value (2) on try {}; expected {}, found {}", tries, find, next);
                                        return Boolean.FALSE;
                                    }
                                    else
                                    {
                                        iter.remove();
                                        notmissing++;
                                    }
                                }
                            }
                            if (find != end)
                            {
                                logger.error("Unexpected last value (2) on try {}; expected {}, found {}", tries, end, find);
                                return Boolean.FALSE;
                            }
                            if (tries > 1 && notmissing < batchSize / 4)
                                break;
                        }
                        if (tries > 1000)
                        {
                            logger.error("Failed to delete 50% of items from the queue, despite 1000 tries (deleted {} of {})", end, find, batchSize - notmissing, batchSize);
                            return Boolean.FALSE;
                        }
                        // poll the entire queue, to permit GC
                        while (queue.poll() != null);

                        // racey stats
                        int da = deleteAttempts.addAndGet(tries - 1);
                        long fd = failedDeletes.addAndGet(notmissing);
                        int bc = totalBatchCount.incrementAndGet();
                        if (bc % 1000 == 0)
                            logger.info("Batch {} of {} Complete. {}% failed deletes on average, after {} tries.", bc, batchCount * THREAD_COUNT, 100 * fd / ((batchSize / 2) * (double) bc), da / (double) bc);
                    }

                    return Boolean.TRUE;
                }
            }));
        }

        for (Future<Boolean> result : success)
            Assert.assertTrue(result.get());
    }

    // similar to testIteratorRemoval, except every thread operates over the same range to test hyper competitive deletes
    @Test
    public void testIteratorRemoval2() throws ExecutionException, InterruptedException
    {
        final NonBlockingQueue<Integer> queue = new NonBlockingQueue<>();
        final List<Future<Boolean>> success = new ArrayList<>();
        final AtomicInteger totalBatchCount = new AtomicInteger();
        final AtomicLong failedDeletes = new AtomicLong();
        final AtomicInteger deleteAttempts = new AtomicInteger();
        for (int i = 0 ; i < THREAD_COUNT ; i++)
        {
            success.add(exec.submit(new Callable<Boolean>()
            {
                @Override
                public Boolean call()
                {
                    final int batchCount = 200;
                    final int batchSize = 1 << 16;
                    for (int batch = 0 ; batch < batchCount ; batch++)
                    {
                        NonBlockingQueueView<Integer> view = queue.view();
                        for (int i = 0 ; i < batchSize ; i += 1)
                            queue.append(i);

                        // snap a range of the queue that should contain all the items we add
                        Iterable<Integer> snap = view.snap();
                        // delete every other item, and loop until they're all gone, failing if we cannot delete more than
                        // 50% over 1000 tries. Note that since we're operating over the same range as other operations here
                        // we delete every instance we see that isn't (mathematically) even
                        int tries = 0;
                        int notmissing = 0;
                        while (++tries <= 1000)
                        {
                            int find = 0;
                            notmissing = 0;
                            Iterator<Integer> iter = snap.iterator();
                            while (iter.hasNext())
                            {
                                Integer next = iter.next();
                                if ((next & 1) == 1)
                                {
                                    iter.remove();
                                    notmissing++;
                                }
                                else if (next == find)
                                    find += 2;
                            }
                            if (find != batchSize)
                            {
                                logger.error("Unexpected last value; expected {}, found {}", batchSize, find);
                                return Boolean.FALSE;
                            }
                            if (tries > 1 && notmissing < batchSize / 4)
                                break;
                        }
                        if (tries > 1000)
                        {
                            logger.error("Failed to delete 50% of items from the queue, despite 1000 tries");
                            return Boolean.FALSE;
                        }
                        // poll the entire queue, to permit GC
                        while (queue.poll() != null);

                        int da = deleteAttempts.addAndGet(tries - 1);
                        long fd = failedDeletes.addAndGet(notmissing);
                        int bc = totalBatchCount.incrementAndGet();
                        if (bc % 1000 == 0)
                            logger.info("Batch {} of {} Complete. {}% failed deletes on average, after {} tries.", bc, batchCount * THREAD_COUNT, 100 * fd / ((batchSize / 2) * (double) bc), da / (double) bc);
                    }

                    return Boolean.TRUE;
                }
            }));
        }

        for (Future<Boolean> result : success)
            Assert.assertTrue(result.get());
    }

    @Test
    public void testSimpleAppendAndPoll() throws ExecutionException, InterruptedException
    {
        final int batchSize = 1 << 20;
        final int batchCount = 10;
        for (int batch = 0 ; batch < batchCount ; batch++)
        {
            final NonBlockingQueue<Integer> queue = new NonBlockingQueue<>();
            final List<Future<int[]>> success = new ArrayList<>();
            for (int i = 0 ; i < THREAD_COUNT ; i++)
            {
                final int offset = i;
                success.add(exec.submit(new Callable<int[]>()
                {
                    @Override
                    public int[] call()
                    {
                        int[] items = new int[batchSize];
                        for (int i = 0 ; i < batchSize ; i++)
                        {
                            queue.append((i * THREAD_COUNT) + offset);
                            items[i] = queue.poll();
                        }
                        return items;
                    }
                }));
            }

            final boolean[] found = new boolean[batchSize * THREAD_COUNT];
            for (Future<int[]> result : success)
            {
                for (int i : result.get())
                {
                    Assert.assertFalse(found[i]);
                    found[i] = true;
                }
            }
            for (boolean b : found)
                Assert.assertTrue(b);
            Assert.assertTrue(queue.isEmpty());
            logger.info("Batch {} of {}", batch + 1, batchCount);
        }
    }

    @Test
    public void testConditionalAppendAndPoll() throws ExecutionException, InterruptedException
    {
        final NonBlockingQueue<Integer> queue = new NonBlockingQueue<>();
        final List<Future<Boolean>> success = new ArrayList<>();
        final AtomicLong totalOps = new AtomicLong();
        final int perThreadOps = 1 << 20;
        queue.append(-1);
        for (int i = 0 ; i < THREAD_COUNT ; i++)
        {
            final int offset = i;
            success.add(exec.submit(new Callable<Boolean>()
            {
                @Override
                public Boolean call()
                {
                    for (int i = 0 ; i < perThreadOps ; i++)
                    {
                        int v = (i * THREAD_COUNT) + offset;
                        while (true)
                        {
                            Snap<Integer> snap = queue.snap();
                            Integer tail = snap.tail();
                            if (queue.appendIfTail(tail, v))
                            {
                                Assert.assertTrue(Iterables.contains(snap.view(), v));
                                while (true)
                                {
                                    Integer peek = queue.peek();
                                    if (peek == tail || peek == v || peek == queue.tail())
                                        break;
                                    queue.advanceHeadIf(peek);
                                }
                                break;
                            }
                            else
                            {
                                Assert.assertFalse(Iterables.contains(snap.view(), v));
                            }
                        }
                        long to = totalOps.incrementAndGet();
                        if ((to & ((1 << 20) - 1)) == 0)
                            logger.info("Completed {}M ops of {}M", to >> 20, (perThreadOps * THREAD_COUNT) >> 20);
                    }
                    return Boolean.TRUE;
                }
            }));
        }

        for (Future<Boolean> result : success)
            Assert.assertTrue(result.get());
    }
}
