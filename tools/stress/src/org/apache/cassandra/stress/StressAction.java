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
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;

import com.google.common.util.concurrent.RateLimiter;
import org.apache.cassandra.stress.operations.*;
import org.apache.cassandra.stress.settings.OpType;
import org.apache.cassandra.stress.settings.StressSettings;
import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.transport.SimpleClient;

public class StressAction
{

    private final StressSettings settings;
    private final PrintStream output;

    public StressAction(StressSettings settings, PrintStream out)
    {
        this.settings = settings;
        output = out;
    }

    public void run()
    {
        // creating keyspace and column families
        settings.maybeCreateKeyspaces();

        // warmup - do 50k iterations; by default hotspot compiles methods after 10k invocations
        PrintStream warmupOutput = new PrintStream(new OutputStream() { @Override public void write(int b) throws IOException { } } );
        output.println("Warming up...");
        for (OpType type : settings.op.type.getWarmups())
            run(type, 20, 50000, warmupOutput);

        output.println("Sleeping 2s...");
        try { Thread.sleep(2000); } catch (InterruptedException e) { }

        if (settings.rate.auto)
            runAuto();
        else
            run(settings.op.type, settings.rate.threads, settings.op.count, output);
    }

    private void runAuto()
    {
        int threadCount = 10;
        List<StressMetrics> results = new ArrayList<>();
        while (true)
        {
            // run until previous two have not been greater than previous plus (uncertainty * 1.5)
            output.println(String.format("Running with %d threads", threadCount));
            StressMetrics result = run(settings.op.type, threadCount, settings.op.count, output);
            results.add(result);
            threadCount *= 1.5;
        }
    }

    private StressMetrics run(OpType type, int threadCount, long opCount, PrintStream output)
    {

        final WorkQueue workQueue;
        if (opCount < 0)
            workQueue = new ContinuousWorkQueue(50);
        else
            workQueue = FixedWorkQueue.build(opCount);

        RateLimiter rateLimiter = null;
        // TODO : move this to a new queue wrapper that gates progress based on a poisson distribution
        if (settings.rate.opLimitPerSecond > 0)
            rateLimiter = RateLimiter.create(settings.rate.opLimitPerSecond);

        final StressMetrics metrics = new StressMetrics(output);

        final CountDownLatch done = new CountDownLatch(threadCount);
        final Consumer[] consumers = new Consumer[threadCount];
        for (int i = 0; i < threadCount; i++)
            consumers[i] = new Consumer(type, done, workQueue, metrics, rateLimiter);

        // starting worker threads
        for (int i = 0; i < threadCount; i++)
            consumers[i].start();

        metrics.meterWithLogInterval(settings.log.intervalMillis);


        if (opCount <= 0)
        {
            try
            {
                metrics.runUntilConverges(settings.op.targetUncertainty, settings.op.minimumUncertaintyMeasurements);
            } catch (InterruptedException e)
            {
                // TODO : terminate gracefully
                throw new IllegalStateException();
            }
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

        private final Operation.State state;
        private final RateLimiter rateLimiter;
        private volatile boolean success = true;
        private final WorkQueue workQueue;
        private final CountDownLatch done;

        public Consumer(OpType type, CountDownLatch done, WorkQueue workQueue, StressMetrics metrics, RateLimiter rateLimiter)
        {
            this.done = done;
            this.rateLimiter = rateLimiter;
            this.workQueue = workQueue;
            this.state = new Operation.State(type, settings, metrics);
        }

        public void run()
        {

            try
            {

                SimpleClient sclient = null;
                Cassandra.Client cclient = null;

                if (settings.mode.useNativeProtocol)
                    sclient = settings.getNativeClient();
                else
                    cclient = settings.getClient();

                Work work;
                while ( null != (work = workQueue.poll()) )
                {

                    if (rateLimiter != null)
                        rateLimiter.acquire(work.count);

                    for (int i = 0 ; i < work.count ; i++)
                    {
                        try
                        {
                            Operation op = createOperation(state, i + work.offset);
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
                            state.metrics.stop();
                            return;
                        }
                    }
                }

            }
            finally
            {
                done.countDown();
                state.timer.close();
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

    // TODO : POISSON DISTRIBUTION WORK QUEUE

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

        static FixedWorkQueue build(long operations)
        {
            // target splitting into around 50-500k items, with a minimum size of 20
            if (operations > Integer.MAX_VALUE * (1 << 9))
                throw new IllegalStateException("Cannot currently support more than approx 2^40 operations for one stress run. This is a LOT.");
            int batchSize = (int) (operations / (1 << 9));
            if (batchSize < 20)
                batchSize = 20;
            ArrayBlockingQueue<Work> work = new ArrayBlockingQueue<Work>(
                    (int) ((operations / batchSize)
                  + (operations % batchSize == 0 ? 0 : 1))
            );
            long offset = 0;
            while (offset < operations)
            {
                work.add(new Work(offset, (int) Math.min(batchSize, operations - offset)));
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

    private Operation createOperation(Operation.State state, long index)
    {
        return createOperation(state.type, state, index);
    }
    private Operation createOperation(OpType type, Operation.State state, long index)
    {
        switch (type)
        {
            case READ:
                switch(state.settings.mode.api)
                {
                    case THRIFT:
                        return new ThriftReader(state, index);
                    case CQL:
                    case CQL_PREPARED:
                        return new CqlReader(state, index);
                    default:
                        throw new UnsupportedOperationException();
                }


            case COUNTERREAD:
                switch(state.settings.mode.api)
                {
                    case THRIFT:
                        return new ThriftCounterGetter(state, index);
                    case CQL:
                    case CQL_PREPARED:
                        return new CqlCounterGetter(state, index);
                    default:
                        throw new UnsupportedOperationException();
                }

            case INSERT:
                switch(state.settings.mode.api)
                {
                    case THRIFT:
                        return new ThriftInserter(state, index);
                    case CQL:
                    case CQL_PREPARED:
                        return new CqlInserter(state, index);
                    default:
                        throw new UnsupportedOperationException();
                }

            case COUNTERWRITE:
                switch(state.settings.mode.api)
                {
                    case THRIFT:
                        return new ThriftCounterAdder(state, index);
                    case CQL:
                    case CQL_PREPARED:
                        return new CqlCounterAdder(state, index);
                    default:
                        throw new UnsupportedOperationException();
                }

            case RANGE_SLICE:
                switch(state.settings.mode.api)
                {
                    case THRIFT:
                        return new ThriftRangeSlicer(state, index);
                    case CQL:
                    case CQL_PREPARED:
                        return new CqlRangeSlicer(state, index);
                    default:
                        throw new UnsupportedOperationException();
                }

            case INDEXED_RANGE_SLICE:
                switch(state.settings.mode.api)
                {
                    case THRIFT:
                        return new ThriftIndexedRangeSlicer(state, index);
                    case CQL:
                    case CQL_PREPARED:
                        // TODO
                        throw new UnsupportedOperationException();
                    default:
                        throw new UnsupportedOperationException();
                }

            case READMULTI:
                switch(state.settings.mode.api)
                {
                    case THRIFT:
                        return new ThriftMultiGetter(state, index);
                    case CQL:
                    case CQL_PREPARED:
                        return new CqlMultiGetter(state, index);
                    default:
                        throw new UnsupportedOperationException();
                }

            case MIXED:
                return createOperation(state.readWriteSelector.next(), state, index);

        }

        throw new UnsupportedOperationException();
    }

}
