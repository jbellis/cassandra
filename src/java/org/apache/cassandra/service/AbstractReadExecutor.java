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

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.concurrent.Stage;
import org.apache.cassandra.concurrent.StageManager;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.config.ReadRepairDecision;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.db.ReadResponse;
import org.apache.cassandra.db.Row;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.exceptions.ReadTimeoutException;
import org.apache.cassandra.exceptions.UnavailableException;
import org.apache.cassandra.metrics.ReadRepairMetrics;
import org.apache.cassandra.net.MessageOut;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.service.StorageProxy.LocalReadRunnable;
import org.apache.cassandra.utils.FBUtilities;

/**
 * Sends a read request to the replicas needed to satisfy a given ConsistencyLevel.
 *
 * Optionally, may perform additional requests to provide redundancy against replica failure:
 * AlwaysSpeculatingReadExecutor will send a request to all replicas, while
 * SpeculatingReadExecutor will wait until it looks like the original request is in danger
 * of timing out befor performing extra reads.
 */
public abstract class AbstractReadExecutor
{
    private static final Logger logger = LoggerFactory.getLogger(AbstractReadExecutor.class);

    protected final ColumnFamilyStore cfs;
    protected final ReadCommand command;
    protected final RowDigestResolver resolver;
    protected final ReadCallback<ReadResponse, Row> handler;

    // The minimal set of replicas required to satisfy the given consistency level.
    protected final List<InetAddress> targetReplicas;

    AbstractReadExecutor(ColumnFamilyStore cfs,
                         ReadCommand command,
                         ConsistencyLevel consistencyLevel,
                         List<InetAddress> targetReplicas)
    throws UnavailableException
    {
        this.cfs = cfs;
        this.command = command;
        this.resolver = new RowDigestResolver(command.ksName, command.key);
        this.handler = new ReadCallback<>(resolver, consistencyLevel, command, targetReplicas);
        handler.assureSufficientLiveNodes();

        this.targetReplicas = targetReplicas;
    }

    private boolean isLocalRequest(InetAddress replica)
    {
        return replica.equals(FBUtilities.getBroadcastAddress()) && StorageProxy.OPTIMIZE_LOCAL_REQUESTS;
    }

    protected void makeDataRequests(List<InetAddress> endpoints)
    {
        for (InetAddress endpoint : endpoints)
        {
            if (isLocalRequest(endpoint))
            {
                logger.trace("reading data locally");
                StageManager.getStage(Stage.READ).execute(new LocalReadRunnable(command, handler));
            }
            else
            {
                logger.trace("reading data from {}", endpoint);
                MessagingService.instance().sendRR(command.createMessage(), endpoint, handler);
            }
        }
    }

    protected void makeDigestRequests(List<InetAddress> endpoints)
    {
        ReadCommand digestCommand = command.copy();
        digestCommand.setDigestQuery(true);
        MessageOut<?> message = digestCommand.createMessage();
        for (InetAddress endpoint : endpoints)
        {
            if (isLocalRequest(endpoint))
            {
                logger.trace("reading digest locally");
                StageManager.getStage(Stage.READ).execute(new LocalReadRunnable(digestCommand, handler));
            }
            else
            {
                logger.trace("reading digest from {}", endpoint);
                MessagingService.instance().sendRR(message, endpoint, handler);
            }
        }
    }

    /**
     * Perform additional requests if it looks like the original will time out.  May block while it waits
     * to see if the original requests are answered first.
     */
    public abstract void maybeTryAdditionalReplicas();

    /**
     * Get the replicas involved in the [finished] request.
     *
     * @return target replicas + the extra replica, *IF* we speculated.
     */
    public abstract Iterable<InetAddress> getContactedReplicas();

    /**
     * send the inital set of requests
     */
    public void executeAsync()
    {
        // Make a full data request to the first endpoint.
        makeDataRequests(targetReplicas.subList(0, 1));
        // Make the digest requests to the other endpoints, if there are any.
        if (targetReplicas.size() > 1)
            makeDigestRequests(targetReplicas.subList(1, targetReplicas.size()));
    }

    /**
     * wait for an answer.  Blocks until success or timeout, so it is caller's
     * responsibility to call maybeTryAdditionalReplicas first.
     */
    public Row get() throws ReadTimeoutException, DigestMismatchException
    {
        return handler.get();
    }

    /**
     * @return an executor appropriate for the configured speculative read policy
     */
    public static AbstractReadExecutor getReadExecutor(ReadCommand command, ConsistencyLevel consistencyLevel) throws UnavailableException
    {
        ReadRepairDecision repairDecision = Schema.instance.getCFMetaData(command.ksName, command.cfName).newReadRepairDecision();
        if (repairDecision != ReadRepairDecision.NONE)
            ReadRepairMetrics.attempted.mark();

        Keyspace keyspace = Keyspace.open(command.ksName);
        List<InetAddress> allReplicas = StorageProxy.getLiveSortedEndpoints(keyspace, command.key);
        List<InetAddress> targetReplicas = consistencyLevel.filterForQuery(keyspace, allReplicas, repairDecision);

        // Fat client.
        if (StorageService.instance.isClientMode())
            return new NeverSpeculatingReadExecutor(null, command, consistencyLevel, targetReplicas);

        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(command.cfName);

        // No extra replicas available or no speculative retry requested.
        if (allReplicas.size() == targetReplicas.size() || cfs.metadata.getSpeculativeRetry().type == CFMetaData.SpeculativeRetry.RetryType.NONE)
            return new NeverSpeculatingReadExecutor(cfs, command, consistencyLevel, targetReplicas);

        // In most cases we can just take the next replica from all replicas as the extra.
        InetAddress extraReplica = allReplicas.get(targetReplicas.size());
        // With repair decision DC_LOCAL, however, we can't, because all replicas/target replicas may be in different order.
        if (repairDecision == ReadRepairDecision.DC_LOCAL)
        {
            for (InetAddress address : allReplicas)
            {
                if (!targetReplicas.contains(address))
                {
                    extraReplica = address;
                    break;
                }
            }
        }

        if (cfs.metadata.getSpeculativeRetry().type == CFMetaData.SpeculativeRetry.RetryType.ALWAYS)
            return new AlwaysSpeculatingReadExecutor(cfs, command, consistencyLevel, targetReplicas, extraReplica);

        // PERCENTILE or CUSTOM.
        return new SpeculatingReadExecutor(cfs, command, consistencyLevel, targetReplicas, extraReplica);
    }

    private static class NeverSpeculatingReadExecutor extends AbstractReadExecutor
    {
        public NeverSpeculatingReadExecutor(ColumnFamilyStore cfs,
                                            ReadCommand command,
                                            ConsistencyLevel consistencyLevel,
                                            List<InetAddress> targetReplicas)
        throws UnavailableException
        {
            super(cfs, command, consistencyLevel, targetReplicas);
        }

        public void maybeTryAdditionalReplicas()
        {
            // no-op
        }

        public Iterable<InetAddress> getContactedReplicas()
        {
            return targetReplicas;
        }
    }

    private static class SpeculatingReadExecutor extends AbstractReadExecutor
    {
        private final InetAddress extraReplica;
        private volatile boolean speculated = false;

        public SpeculatingReadExecutor(ColumnFamilyStore cfs,
                                       ReadCommand command,
                                       ConsistencyLevel consistencyLevel,
                                       List<InetAddress> targetReplicas,
                                       InetAddress extraReplica)
        throws UnavailableException
        {
            super(cfs, command, consistencyLevel, targetReplicas);
            this.extraReplica = extraReplica;
        }

        public void maybeTryAdditionalReplicas()
        {
            // no latency information, or we're overloaded
            if (cfs.sampleLatency > TimeUnit.MILLISECONDS.toNanos(command.getTimeout()))
                return;

            if (!handler.await(cfs.sampleLatency, TimeUnit.NANOSECONDS))
            {
                // Could be waiting on the data, or on enough digests.
                ReadCommand retryCommand = command;
                if (resolver.getData() != null)
                {
                    retryCommand = command.copy();
                    retryCommand.setDigestQuery(true);
                }

                logger.trace("speculating read retry on {}", extraReplica);
                MessagingService.instance().sendRR(retryCommand.createMessage(), extraReplica, handler);
                speculated = true;
                cfs.metric.speculativeRetry.inc();
            }
        }

        public Iterable<InetAddress> getContactedReplicas()
        {
            return speculated
                   ? Iterables.concat(targetReplicas, Collections.singleton(extraReplica))
                   : targetReplicas;
        }
    }

    private static class AlwaysSpeculatingReadExecutor extends AbstractReadExecutor
    {
        private final InetAddress extraReplica;

        public AlwaysSpeculatingReadExecutor(ColumnFamilyStore cfs,
                                             ReadCommand command,
                                             ConsistencyLevel consistencyLevel,
                                             List<InetAddress> targetReplicas,
                                             InetAddress extraReplica)
        throws UnavailableException
        {
            super(cfs, command, consistencyLevel, targetReplicas);
            this.extraReplica = extraReplica;
        }

        public void maybeTryAdditionalReplicas()
        {
            // no-op
        }

        public Iterable<InetAddress> getContactedReplicas()
        {
            return Iterables.concat(targetReplicas, Collections.singleton(extraReplica));
        }

        public void executeAsync()
        {
            // Make TWO data requests - to the first two endpoints.
            List<InetAddress> dataEndpoints = targetReplicas.size() > 1
                                            ? targetReplicas.subList(0, 2)
                                            : Lists.newArrayList(targetReplicas.get(0), extraReplica);
            makeDataRequests(dataEndpoints);

            // Make digest requests to [the rest of the target replicas + the extra replica], if any.
            if (targetReplicas.size() > 1)
            {
                List<InetAddress> digestEndpoints = new ArrayList<>(targetReplicas.subList(2, targetReplicas.size()));
                digestEndpoints.add(extraReplica);
                makeDigestRequests(digestEndpoints);
            }
        }
    }
}
