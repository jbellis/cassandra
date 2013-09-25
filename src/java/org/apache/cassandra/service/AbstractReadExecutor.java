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
import java.util.List;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Optional;
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

public abstract class AbstractReadExecutor
{
    private static final Logger logger = LoggerFactory.getLogger(AbstractReadExecutor.class);

    protected final ColumnFamilyStore cfs;
    protected final ReadCommand command;
    protected final RowDigestResolver resolver;
    protected final ReadCallback<ReadResponse, Row> handler;

    // The minimal set of replicas required to satisfy the given consistency level.
    protected final List<InetAddress> targetReplicas;
    // The extra replica for speculative retries.
    protected final Optional<InetAddress> extraReplica;

    AbstractReadExecutor(ColumnFamilyStore cfs,
                         ReadCommand command,
                         ConsistencyLevel consistencyLevel,
                         List<InetAddress> targetReplicas,
                         Optional<InetAddress> extraReplica)
    throws UnavailableException
    {
        this.cfs = cfs;
        this.command = command;
        this.resolver = new RowDigestResolver(command.ksName, command.key);
        this.handler = new ReadCallback<>(resolver, consistencyLevel, command, targetReplicas);
        handler.assureSufficientLiveNodes();

        this.targetReplicas = targetReplicas;
        this.extraReplica = extraReplica;
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

    abstract void speculate();

    abstract boolean speculated();

    /**
     * Get the replicas involved in the [finished] request.
     *
     * @return target replicas + the extra replica, *IF* we speculated.
     */
    List<InetAddress> getContactedReplicas()
    {
        if (!speculated())
            return targetReplicas;

        List<InetAddress> replicas = new ArrayList<>(targetReplicas);
        replicas.add(extraReplica.get());
        return replicas;
    }

    void executeAsync()
    {
        // Make a full data request to the first endpoint.
        makeDataRequests(targetReplicas.subList(0, 1));
        // Make the digest requests to the other endpoints, if there are any.
        if (targetReplicas.size() > 1)
            makeDigestRequests(targetReplicas.subList(1, targetReplicas.size()));
    }

    Row get() throws ReadTimeoutException, DigestMismatchException
    {
        return handler.get();
    }

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
            for (InetAddress address : allReplicas)
                if (!targetReplicas.contains(address))
                {
                    extraReplica = address;
                    break;
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
            super(cfs, command, consistencyLevel, targetReplicas, Optional.<InetAddress>absent());
        }

        void speculate()
        {
            // no-op
        }

        boolean speculated()
        {
            return false;
        }
    }

    private static class SpeculatingReadExecutor extends AbstractReadExecutor
    {
        private volatile boolean speculated = false;

        public SpeculatingReadExecutor(ColumnFamilyStore cfs,
                                       ReadCommand command,
                                       ConsistencyLevel consistencyLevel,
                                       List<InetAddress> targetReplicas,
                                       InetAddress extraReplica)
        throws UnavailableException
        {
            super(cfs, command, consistencyLevel, targetReplicas, Optional.of(extraReplica));
        }

        void speculate()
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

                logger.trace("speculating read retry on {}", extraReplica.get());
                MessagingService.instance().sendRR(retryCommand.createMessage(), extraReplica.get(), handler);
                speculated = true;
                cfs.metric.speculativeRetry.inc();
            }
        }

        @Override
        boolean speculated()
        {
            return speculated;
        }
    }

    private static class AlwaysSpeculatingReadExecutor extends AbstractReadExecutor
    {
        public AlwaysSpeculatingReadExecutor(ColumnFamilyStore cfs,
                                             ReadCommand command,
                                             ConsistencyLevel consistencyLevel,
                                             List<InetAddress> targetReplicas,
                                             InetAddress extraReplica)
        throws UnavailableException
        {
            super(cfs, command, consistencyLevel, targetReplicas, Optional.of(extraReplica));
        }

        void speculate()
        {
            // no-op
        }

        boolean speculated()
        {
            return true;
        }

        @Override
        void executeAsync()
        {
            // Make TWO data requests - to the first two endpoints.
            List<InetAddress> dataEndpoints = targetReplicas.size() > 1
                                            ? targetReplicas.subList(0, 2)
                                            : Lists.newArrayList(targetReplicas.get(0), extraReplica.get());
            makeDataRequests(dataEndpoints);

            // Make digest requests to [the rest of the target replicas + the extra replica], if any.
            if (targetReplicas.size() > 1)
            {
                List<InetAddress> digestEndpoints = new ArrayList<>(targetReplicas.subList(2, targetReplicas.size()));
                digestEndpoints.add(extraReplica.get());
                makeDigestRequests(digestEndpoints);
            }
        }
    }
}
