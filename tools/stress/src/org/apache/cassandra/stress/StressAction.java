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

import java.io.PrintStream;
import java.nio.ByteBuffer;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import com.google.common.util.concurrent.Uninterruptibles;
import com.google.common.util.concurrent.RateLimiter;
import com.yammer.metrics.stats.Snapshot;
import org.apache.cassandra.stress.operations.*;
import org.apache.cassandra.stress.util.CassandraClient;
import org.apache.cassandra.stress.util.Operation;
import org.apache.cassandra.transport.SimpleClient;

public class StressAction extends Thread
{
    /**
     * Producer-Consumer model: 1 producer, N consumers
     */
    private final Session client;
    private final PrintStream output;

    private volatile boolean stop = false;

    public static final int SUCCESS = 0;
    public static final int FAILURE = 1;

    private volatile int returnCode = -1;

    public StressAction(Session session, PrintStream out)
    {
        client = session;
        output = out;
    }

    public void run()
    {
        Snapshot latency;
        long oldLatency;
        int epoch, total, oldTotal, keyCount, oldKeyCount;

        // creating keyspace and column families
        if (client.getOperation() == Stress.Operations.INSERT || client.getOperation() == Stress.Operations.COUNTER_ADD)
            client.createKeySpaces();

        int threadCount = client.getThreads();
        Consumer[] consumers = new Consumer[threadCount];

        output.println("total,interval_op_rate,interval_key_rate,mean,median,95th,99th,elapsed_time");

        int batchSize = 50;
        // creating required type of the threads for the test
        RateLimiter rateLimiter = RateLimiter.create(client.getMaxOpsPerSecond() / (threadCount * batchSize));
        BlockingQueue<Integer> opOffsets = new ArrayBlockingQueue<>(client.getNumKeys() / batchSize);
        for (int i = 0 ; i < client.getNumKeys() ; i += batchSize)
            opOffsets.add(i);

        for (int i = 0; i < threadCount; i++)
            consumers[i] = new Consumer(opOffsets, batchSize, rateLimiter);

        // starting worker threads
        for (int i = 0; i < threadCount; i++)
            consumers[i].start();

        // initialization of the values
        boolean terminate = false;
        epoch = total = keyCount = 0;

        int interval = client.getProgressInterval();
        int epochIntervals = client.getProgressInterval() * 10;
        long testStartTime = System.nanoTime();
        
        StressStatistics stats = new StressStatistics(client, output);

        while (!terminate)
        {
            if (stop)
            {
                for (Consumer consumer : consumers)
                    consumer.stopConsume();

                break;
            }

            Uninterruptibles.sleepUninterruptibly(100, TimeUnit.MILLISECONDS);

            int alive = 0;
            for (Thread thread : consumers)
                if (thread.isAlive()) alive++;

            if (alive == 0)
                terminate = true;

            epoch++;

            if (terminate || epoch > epochIntervals)
            {
                epoch = 0;

                oldTotal = total;
                oldKeyCount = keyCount;

                total = client.operations.get();
                keyCount = client.keys.get();
                latency = client.latency.getSnapshot();
                double meanLatency = client.latency.mean();

                int opDelta = total - oldTotal;
                int keyDelta = keyCount - oldKeyCount;

                long currentTimeInSeconds = TimeUnit.NANOSECONDS.toSeconds(System.nanoTime() - testStartTime);

                output.println(String.format("%d,%d,%d,%.1f,%.1f,%.1f,%.1f,%d",
                                             total,
                                             opDelta / interval,
                                             keyDelta / interval,
                                             meanLatency, latency.getMedian(), latency.get95thPercentile(), latency.get999thPercentile(),
                                             currentTimeInSeconds));

                if (client.outputStatistics()) {
                    stats.addIntervalStats(total, 
                                           opDelta / interval, 
                                           keyDelta / interval, 
                                           latency, meanLatency,
                                           currentTimeInSeconds);
                        }
            }
        }

        // if any consumer failed, set the return code to failure.
        returnCode = SUCCESS;
        for (Consumer consumer : consumers)
            if (consumer.getReturnCode() == FAILURE)
                returnCode = FAILURE;

        if (returnCode == SUCCESS) {            
            if (client.outputStatistics())
                stats.printStats();
            // marking an end of the output to the client
            output.println("END");            
        } else {
            output.println("FAILURE");
        }

    }

    public int getReturnCode()
    {
        return returnCode;
    }

    /**
     * Each consumes exactly N items from queue
     */
    private class Consumer extends Thread
    {
        private final BlockingQueue<Integer> opOffsets;
        private final int batchSize;
        private final RateLimiter rateLimiter;
        private volatile boolean stop = false;
        private volatile int returnCode = StressAction.SUCCESS;

        public Consumer(BlockingQueue<Integer> opOffsets, int batchSize, RateLimiter rateLimiter)
        {
            this.opOffsets = opOffsets;
            this.batchSize = batchSize;
            this.rateLimiter = rateLimiter;
        }

        public void run()
        {

            Random rnd = new Random();
            SimpleClient sclient = null;
            CassandraClient cclient = null;

            if (client.use_native_protocol)
                sclient = client.getNativeClient();
            else
                cclient = client.getClient();

            Integer opOffset;
            while ( null != (opOffset = opOffsets.poll()) )
            {
                if (stop)
                    break;

                rateLimiter.acquire(batchSize);
                for (int i = 0 ; i < batchSize ; i++)
                    try
                    {
                        Operation op = createOperation((i + opOffset) % client.getNumDifferentKeys());
                        if (sclient != null)
                            op.run(sclient);
                        else
                            op.run(cclient);
                    } catch (Exception e)
                    {
                        if (output == null)
                        {
                            System.err.println(e.getMessage());
                            returnCode = StressAction.FAILURE;
                            System.exit(-1);
                        }

                        output.println(e.getMessage());
                        returnCode = StressAction.FAILURE;
                        break;
                    }
            }

        }

        public void stopConsume()
        {
            stop = true;
        }

        public int getReturnCode()
        {
            return returnCode;
        }
    }

    private Operation createOperation(int index)
    {
        switch (client.getOperation())
        {
            case READ:
                return client.isCQL() ? new CqlReader(client, index) : new Reader(client, index);

            case COUNTER_GET:
                return client.isCQL() ? new CqlCounterGetter(client, index) : new CounterGetter(client, index);

            case INSERT:
                return client.isCQL() ? new CqlInserter(client, index) : new Inserter(client, index);

            case COUNTER_ADD:
                return client.isCQL() ? new CqlCounterAdder(client, index) : new CounterAdder(client, index);

            case RANGE_SLICE:
                return client.isCQL() ? new CqlRangeSlicer(client, index) : new RangeSlicer(client, index);

            case INDEXED_RANGE_SLICE:
                return client.isCQL() ? new CqlIndexedRangeSlicer(client, index) : new IndexedRangeSlicer(client, index);

            case MULTI_GET:
                return client.isCQL() ? new CqlMultiGetter(client, index) : new MultiGetter(client, index);
        }

        throw new UnsupportedOperationException();
    }

    public void stopAction()
    {
        stop = true;
    }
}
