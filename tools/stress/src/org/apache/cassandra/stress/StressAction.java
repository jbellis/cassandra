/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.cassandra.stress;

import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.io.StringWriter;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import com.google.common.util.concurrent.RateLimiter;
import org.apache.cassandra.stress.operations.*;
import org.apache.cassandra.stress.util.CassandraClient;
import org.apache.cassandra.stress.util.Operation;
import org.apache.cassandra.transport.SimpleClient;

public class StressAction
{

    private final Session session;
    private final PrintStream output;

    public StressAction(Session session, PrintStream out)
    {
        this.session = session;
        output = out;
    }

    public void run()
    {
        // creating keyspace and column families
        if (session.getOperation() == Stress.Operations.INSERT || session.getOperation() == Stress.Operations.COUNTER_ADD)
            session.createKeySpaces();

        // warmup
        PrintStream warmupOutput = new PrintStream(new OutputStream() { @Override public void write(int b) throws IOException { } } );
        output.println("Warming up...");
        switch (session.getOperation())
        {
            case READWRITE:
                run(Stress.Operations.INSERT, 20, 50000, warmupOutput);
                run(Stress.Operations.READ, 20, 50000, warmupOutput);
                break;
            default:
                run(session.getOperation(), 20, 50000, warmupOutput);
        }

        output.println("Sleeping 2s...");
        try { Thread.sleep(2000); } catch (InterruptedException e) { }

        if (session.auto)
            runAuto();
        else
            run(session.getOperation(), session.getThreads(), session.getNumOperations(), output);
    }

    private void runAuto()
    {

    }

    private StressMetrics run(Stress.Operations kind, int threadCount, int opCount, PrintStream output)
    {

        final WorkQueue workQueue;
        if (opCount < 0)
            workQueue = new ContinuousWorkQueue(50);
        else
            workQueue = FixedWorkQueue.build(opCount);

        RateLimiter rateLimiter = null;
        if (session.getMaxOpsPerSecond() < 1000000000d)
            rateLimiter = RateLimiter.create(session.getMaxOpsPerSecond());

        final StressMetrics metrics = new StressMetrics(output);

        final CountDownLatch done = new CountDownLatch(threadCount);
        final Consumer[] consumers = new Consumer[threadCount];
        for (int i = 0; i < threadCount; i++)
            consumers[i] = new Consumer(kind, done, workQueue, metrics, rateLimiter);

        // starting worker threads
        for (int i = 0; i < threadCount; i++)
            consumers[i].start();

        metrics.meterWithLogInterval(1000 * session.getProgressInterval());

        if (opCount <= 0)
        {
            metrics.runUntilConverges();
            workQueue.stop();
        }

        try
        {
            done.await();
        } catch (InterruptedException e)
        {
            throw new IllegalStateException();
        }

        metrics.stop();
        metrics.summarise();

        boolean success = true;
        for (Consumer consumer : consumers)
            success &= consumer.success;

        if (success) {
            // marking an end of the output to the client
            output.println("END");
        } else {
            output.println("FAILURE");
            System.exit(-1);
        }

        return metrics;
    }

    private class Consumer extends Thread
    {

        private final Operation.Settings settings;
        private final RateLimiter rateLimiter;
        private volatile boolean success = true;
        private final WorkQueue workQueue;
        private final CountDownLatch done;

        public Consumer(Stress.Operations kind, CountDownLatch done, WorkQueue workQueue, StressMetrics metrics, RateLimiter rateLimiter)
        {
            this.done = done;
            this.rateLimiter = rateLimiter;
            this.workQueue = workQueue;
            this.settings = new Operation.Settings(kind, session, metrics);
        }

        public void run()
        {

            try
            {

                SimpleClient sclient = null;
                CassandraClient cclient = null;
                final int uniqueKeyCount = session.getNumDifferentKeys();

                if (session.use_native_protocol)
                    sclient = session.getNativeClient();
                else
                    cclient = session.getClient();

                Work work;
                while ( null != (work = workQueue.poll()) )
                {

                    if (rateLimiter != null)
                        rateLimiter.acquire(work.count);

                    for (int i = 0 ; i < work.count ; i++)
                    {
                        try
                        {
                            Operation op = createOperation(settings, (i + work.offset) % uniqueKeyCount);
                            if (sclient != null)
                                op.run(sclient);
                            else
                                op.run(cclient);
                        } catch (Exception e)
                        {
                            if (output == null)
                            {
                                System.err.println(e.getMessage());
                                success = false;
                                System.exit(-1);
                            }

                            e.printStackTrace(output);
                            success = false;
                            workQueue.stop();
                            settings.metrics.stop();
                            return;
                        }
                    }
                }

            }
            finally
            {
                done.countDown();
                settings.timer.close();
            }

        }

    }

    private interface WorkQueue
    {
        // null indicates consumer should terminate
        Work poll();

        // signal all consumers to terminate
        void stop();
    }

    private static final class Work
    {
        // index of operations
        final long offset;

        // how many operations to perform
        final int count;

        public Work(long offset, int count)
        {
            this.offset = offset;
            this.count = count;
        }
    }

    private static final class FixedWorkQueue implements WorkQueue
    {

        final ArrayBlockingQueue<Work> work;
        volatile boolean stop = false;

        public FixedWorkQueue(ArrayBlockingQueue<Work> work)
        {
            this.work = work;
        }

        @Override
        public Work poll()
        {
            if (stop)
                return null;
            return work.poll();
        }

        @Override
        public void stop()
        {
            stop = true;
        }

        static FixedWorkQueue build(int operations)
        {
            // target splitting into around 50-500k items, with a minimum size of 20
            int batchSize = operations / 500000;
            if (batchSize < 20)
                batchSize = 20;
            ArrayBlockingQueue<Work> work = new ArrayBlockingQueue<Work>(
                    (operations / batchSize)
                  + (operations % batchSize == 0 ? 0 : 1)
            );
            int offset = 0;
            while (offset < operations)
            {
                work.add(new Work(offset, Math.min(batchSize, operations - offset)));
                offset += batchSize;
            }
            return new FixedWorkQueue(work);
        }

    }

    private static final class ContinuousWorkQueue implements WorkQueue
    {

        final AtomicLong offset = new AtomicLong();
        final int batchSize;
        volatile boolean stop = false;

        private ContinuousWorkQueue(int batchSize)
        {
            this.batchSize = batchSize;
        }

        @Override
        public Work poll()
        {
            if (stop)
                return null;
            return new Work(nextOffset(), batchSize);
        }

        private long nextOffset()
        {
            final int inc = batchSize;
            while (true)
            {
                final long cur = offset.get();
                if (offset.compareAndSet(cur, cur + inc))
                    return cur;
            }
        }

        @Override
        public void stop()
        {
            stop = true;
        }

    }

    private Operation createOperation(Operation.Settings settings, long index)
    {
        switch (settings.kind)
        {
            case READ:
                switch(settings.connectionApi)
                {
                    case THRIFT:
                        return new Reader(settings, index);
                    case CQL:
                    case CQL_PREPARED:
                        return new CqlReader(settings, index);
                    default:
                        throw new UnsupportedOperationException();
                }


            case COUNTER_GET:
                switch(settings.connectionApi)
                {
                    case THRIFT:
                        return new CounterGetter(settings, index);
                    case CQL:
                    case CQL_PREPARED:
                        return new CqlCounterGetter(settings, index);
                    default:
                        throw new UnsupportedOperationException();
                }

            case INSERT:
                switch(settings.connectionApi)
                {
                    case THRIFT:
                        return new Inserter(settings, index);
                    case CQL:
                    case CQL_PREPARED:
                        return new CqlInserter(settings, index);
                    default:
                        throw new UnsupportedOperationException();
                }

            case COUNTER_ADD:
                switch(settings.connectionApi)
                {
                    case THRIFT:
                        return new CounterAdder(settings, index);
                    case CQL:
                    case CQL_PREPARED:
                        return new CqlCounterAdder(settings, index);
                    default:
                        throw new UnsupportedOperationException();
                }

            case RANGE_SLICE:
                switch(settings.connectionApi)
                {
                    case THRIFT:
                        return new RangeSlicer(settings, index);
                    case CQL:
                    case CQL_PREPARED:
                        return new CqlRangeSlicer(settings, index);
                    default:
                        throw new UnsupportedOperationException();
                }

            case INDEXED_RANGE_SLICE:
                switch(settings.connectionApi)
                {
                    case THRIFT:
                        return new IndexedRangeSlicer(settings, index);
                    case CQL:
                    case CQL_PREPARED:
                        // TODO
                        throw new UnsupportedOperationException();
                    default:
                        throw new UnsupportedOperationException();
                }

            case MULTI_GET:
                switch(settings.connectionApi)
                {
                    case THRIFT:
                        return new MultiGetter(settings, index);
                    case CQL:
                    case CQL_PREPARED:
                        return new CqlMultiGetter(settings, index);
                    default:
                        throw new UnsupportedOperationException();
                }


        }

        throw new UnsupportedOperationException();
    }

}
