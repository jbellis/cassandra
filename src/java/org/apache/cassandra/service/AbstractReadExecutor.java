/*
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
package org.apache.cassandra.service;

import java.io.IOException;
import java.net.InetAddress;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

import org.apache.cassandra.concurrent.Stage;
import org.apache.cassandra.concurrent.StageManager;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.db.ReadResponse;
import org.apache.cassandra.db.Row;
import org.apache.cassandra.db.Table;
import org.apache.cassandra.exceptions.ReadTimeoutException;
import org.apache.cassandra.exceptions.UnavailableException;
import org.apache.cassandra.net.MessageOut;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.service.StorageProxy.LocalReadRunnable;
import org.apache.cassandra.utils.FBUtilities;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.primitives.Longs;

public abstract class AbstractReadExecutor
{
    private static final Logger logger = LoggerFactory.getLogger(AbstractReadExecutor.class);
    protected final ReadCallback<ReadResponse, Row> handler;
    protected final ReadCommand command;
    protected final RowDigestResolver resolver;
    protected final List<InetAddress> unfiltered;
    protected final List<InetAddress> endpoints;
    protected final ColumnFamilyStore cfs;

    AbstractReadExecutor(ReadCommand command, ConsistencyLevel consistency_level, ColumnFamilyStore cfs) throws UnavailableException
    {
        Table table = Table.open(command.table);
        this.unfiltered = StorageProxy.getLiveSortedEndpoints(table, command.key);
        CFMetaData cfm = Schema.instance.getCFMetaData(command.getKeyspace(), command.getColumnFamilyName());
        this.endpoints = consistency_level.filterForQuery(table, unfiltered, cfm.newReadRepairDecision());
        this.resolver = new RowDigestResolver(command.table, command.key);
        this.handler = new ReadCallback<ReadResponse, Row>(resolver, consistency_level, command, endpoints);
        this.command = command;
        handler.assureSufficientLiveNodes();
        assert !handler.endpoints.isEmpty();
        this.cfs = cfs;
    }

    void executeAsync()
    {
        // The data-request message is sent to dataPoint, the node that will actually get the data for us
        InetAddress dataPoint = handler.endpoints.get(0);
        if (dataPoint.equals(FBUtilities.getBroadcastAddress()) && StorageProxy.OPTIMIZE_LOCAL_REQUESTS)
        {
            logger.trace("reading data locally");
            StageManager.getStage(Stage.READ).execute(new LocalReadRunnable(command, handler));
        }
        else
        {
            logger.trace("reading data from {}", dataPoint);
            MessagingService.instance().sendRR(command.createMessage(), dataPoint, handler);
        }

        if (handler.endpoints.size() == 1)
            return;

        // send the other endpoints a digest request
        ReadCommand digestCommand = command.copy();
        digestCommand.setDigestQuery(true);
        MessageOut<?> message = null;
        for (int i = 1; i < handler.endpoints.size(); i++)
        {
            InetAddress digestPoint = handler.endpoints.get(i);
            if (digestPoint.equals(FBUtilities.getBroadcastAddress()))
            {
                logger.trace("reading digest locally");
                StageManager.getStage(Stage.READ).execute(new LocalReadRunnable(digestCommand, handler));
            }
            else
            {
                logger.trace("reading digest from {}", digestPoint);
                // (We lazy-construct the digest Message object since it may not be necessary if we
                // are doing a local digest read, or no digest reads at all.)
                if (message == null)
                    message = digestCommand.createMessage();
                MessagingService.instance().sendRR(message, digestPoint, handler);
            }
        }
    }

    void speculate()
    {
        // noop by default.
    }

    Row get() throws ReadTimeoutException, DigestMismatchException, IOException
    {
        return handler.get();
    }

    public static AbstractReadExecutor getReadExecutor(ReadCommand command, ConsistencyLevel consistency_level) throws UnavailableException
    {
        ColumnFamilyStore cfs = Table.open(command.table).getColumnFamilyStore(command.getColumnFamilyName());
        switch (cfs.metadata.getSpeculativeRetry().type)
        {
        case ALWAYS:
            return new SpeculateAlwaysExecutor(command, consistency_level, cfs);
        case PERCENTILE:
        case CUSTOM:
            return new SpeculativeReadExecutor(command, consistency_level, cfs);
        default:
            return new DefaultReadExecutor(command, consistency_level, cfs);
        }
    }

    public static void sortByExpectedLatency(AbstractReadExecutor[] execs) {
        Arrays.sort(execs, new Comparator<AbstractReadExecutor>()
        {
            public int compare(AbstractReadExecutor o1, AbstractReadExecutor o2)
            {
                long a = o1.cfs.sampleLatency;
                long b = o2.cfs.sampleLatency;
                // if sample latency is disabled push it to the bottom 
                if (a == 0)
                    return 1;
                else if (b == 0)
                    return -1;
                return Longs.compare(a, b);
            }
        });
    }
    private static class DefaultReadExecutor extends AbstractReadExecutor
    {
        DefaultReadExecutor(ReadCommand command, ConsistencyLevel consistency_level, ColumnFamilyStore cfs) throws UnavailableException
        {
            super(command, consistency_level, cfs);
        }
    }

    private static class SpeculativeReadExecutor extends AbstractReadExecutor
    {
        public SpeculativeReadExecutor(ReadCommand command, ConsistencyLevel consistency_level, ColumnFamilyStore cfs) throws UnavailableException
        {
            super(command, consistency_level, cfs);
        }

        @Override
        void speculate()
        {
            long expectedLatency = cfs.sampleLatency;
            if (expectedLatency == 0 || expectedLatency > command.getTimeout())
                return;

            if (!handler.await(expectedLatency))
            {
                InetAddress endpoint;
                if (unfiltered.size() > handler.endpoints.size())
                    endpoint = unfiltered.get(handler.endpoints.size()); // read != RR
                else if (unfiltered.size() > 1)
                    endpoint = unfiltered.get(1); // read == RR, re-send data read to a different node.
                else
                    return;

                ReadCommand scommand = command;
                if (resolver.getData() != null) // check if we have to send digest request
                {
                    scommand = command.copy();
                    scommand.setDigestQuery(true);
                }
                logger.trace("Speculating read retry on {}", endpoint);
                MessagingService.instance().sendRR(scommand.createMessage(), endpoint, handler);
                cfs.metric.speculativeRetry.inc();
            }
        }
    }

    private static class SpeculateAlwaysExecutor extends AbstractReadExecutor
    {
        public SpeculateAlwaysExecutor(ReadCommand command, ConsistencyLevel consistency_level, ColumnFamilyStore cfs) throws UnavailableException
        {
            super(command, consistency_level, cfs);
        }

        @Override
        void executeAsync()
        {
            int limit = unfiltered.size() >= 2 ? 2 : 1;
            for (int i = 0; i < limit; i++)
            {
                InetAddress endpoint = unfiltered.get(i);
                if (endpoint.equals(FBUtilities.getBroadcastAddress()))
                {
                    logger.trace("reading full data locally");
                    StageManager.getStage(Stage.READ).execute(new LocalReadRunnable(command, handler));
                }
                else
                {
                    logger.trace("reading full data from {}", endpoint);
                    MessagingService.instance().sendRR(command.createMessage(), endpoint, handler);
                }
            }
            if (handler.endpoints.size() <= limit)
                return;

            ReadCommand digestCommand = command.copy();
            digestCommand.setDigestQuery(true);
            MessageOut<?> message = digestCommand.createMessage();
            for (int i = limit; i < handler.endpoints.size(); i++)
            {
                // Send the message
                InetAddress endpoint = handler.endpoints.get(i);
                if (endpoint.equals(FBUtilities.getBroadcastAddress()))
                {
                    logger.trace("reading data locally, isDigest: {}", command.isDigestQuery());
                    StageManager.getStage(Stage.READ).execute(new LocalReadRunnable(digestCommand, handler));
                }
                else
                {
                    logger.trace("reading full data from {}, isDigest: {}", endpoint, command.isDigestQuery());
                    MessagingService.instance().sendRR(message, endpoint, handler);
                }
            }
        }
    }
}
