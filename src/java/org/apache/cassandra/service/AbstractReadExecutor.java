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
import java.util.List;

import org.apache.cassandra.concurrent.Stage;
import org.apache.cassandra.concurrent.StageManager;
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

public abstract class AbstractReadExecutor
{
    private static final Logger logger = LoggerFactory.getLogger(AbstractReadExecutor.class);
    protected final ReadCallback<ReadResponse, Row> handler;
    protected final ReadCommand command;
    protected final RowDigestResolver resolver;

    AbstractReadExecutor(ReadCommand command, ConsistencyLevel consistency_level) throws UnavailableException
    {
        Table table = Table.open(command.table);
        List<InetAddress> endpoints = StorageProxy.getLiveSortedEndpoints(table, command.key);
        this.resolver = new RowDigestResolver(command.table, command.key);
        this.handler = new ReadCallback<ReadResponse, Row>(resolver, consistency_level, command, endpoints);
        this.command = command;

        handler.assureSufficientLiveNodes();
        assert !handler.endpoints.isEmpty();
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

    Row get() throws ReadTimeoutException, DigestMismatchException, IOException
    {
        return handler.get();
    }

    public static AbstractReadExecutor getReadExecutor(ReadCommand command, ConsistencyLevel consistency_level) throws UnavailableException
    {
        return new DefaultReadExecutor(command, consistency_level);
    }

    private static class DefaultReadExecutor extends AbstractReadExecutor
    {
        DefaultReadExecutor(ReadCommand command, ConsistencyLevel consistency_level) throws UnavailableException
        {
            super(command, consistency_level);
        }
    }
}
