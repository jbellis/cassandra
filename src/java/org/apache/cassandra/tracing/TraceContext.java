/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */
package org.apache.cassandra.tracing;

import static com.google.common.base.Preconditions.checkState;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOError;
import java.io.IOException;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.RejectedExecutionException;

import org.apache.cassandra.concurrent.Stage;
import org.apache.cassandra.concurrent.StageManager;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.cql3.ColumnNameBuilder;
import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.ExpiringColumn;
import org.apache.cassandra.db.RowMutation;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.TimeUUIDType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.net.MessageIn;
import org.apache.cassandra.net.MessageOut;
import org.apache.cassandra.service.StorageProxy;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.UUIDGen;

import com.google.common.annotations.VisibleForTesting;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A trace session context. Able to track and store trace sessions. A session is usually a user initiated query, and may
 * have multiple local and remote events before it is completed. All events and sessions are stored at table.
 */
public class TraceContext
{
    public static final String TRACE_KS = "system_traces";
    public static final String EVENTS_CF = "events";
    public static final String TRACE_HEADER = "TraceSession";

    private static final int TTL = 24 * 3600;

    private static TraceContext instance = new TraceContext();

    public static final String COORDINATOR = "coordinator";
    public static final String DESCRIPTION = "description";
    public static final ByteBuffer DESCRIPTION_BB = ByteBufferUtil.bytes(DESCRIPTION);
    public static final String DURATION = "duration";
    public static final ByteBuffer DURATION_BB = ByteBufferUtil.bytes(DURATION);
    public static final String EVENT_ID = "eventId";
    public static final String HAPPENED = "happened_at";
    public static final ByteBuffer HAPPENED_BB = ByteBufferUtil.bytes(HAPPENED);
    public static final String NAME = "name";
    public static final ByteBuffer NAME_BB = ByteBufferUtil.bytes(NAME);
    public static final String PAYLOAD = "payload";
    public static final ByteBuffer PAYLOAD_BB = ByteBufferUtil.bytes(PAYLOAD);
    public static final String PAYLOAD_TYPES = "payload_types";
    public static final ByteBuffer PAYLOAD_TYPES_BB = ByteBufferUtil.bytes(PAYLOAD_TYPES);
    public static final String SESSION_ID = "sessionId";
    public static final String SOURCE = "source";
    public static final ByteBuffer SOURCE_BB = ByteBufferUtil.bytes(SOURCE);
    public static final String TYPE = "type";
    public static final ByteBuffer TYPE_BB = ByteBufferUtil.bytes(TYPE);

    private static final Logger logger = LoggerFactory.getLogger(TraceContext.class);

    @VisibleForTesting
    public static void setInstance(TraceContext context)
    {
        instance = context;
    }

    /**
     * Fetches and lazy initializes the trace context.
     */
    public static TraceContext instance()
    {
        return instance;
    }

    private InetAddress localAddress;
    private final ThreadLocal<TraceState> state = new ThreadLocal<TraceState>();

    private void addColumn(ColumnFamily cf, ByteBuffer columnName, InetAddress address)
    {
        cf.addColumn(new ExpiringColumn(columnName, bytes(address), System.currentTimeMillis(), TTL));
    }

    private void addColumn(ColumnFamily cf, ByteBuffer columnName, long value)
    {
        cf.addColumn(new ExpiringColumn(columnName, ByteBufferUtil.bytes(value), System.currentTimeMillis(), TTL));
    }

    private void addColumn(ColumnFamily cf, ByteBuffer columnName, String value)
    {
        cf.addColumn(new ExpiringColumn(columnName, ByteBufferUtil.bytes(value), System.currentTimeMillis(), TTL));
    }

    private void addPayloadColumns(ColumnFamily cf, Map<String, ByteBuffer> rawPayload,
            ByteBuffer coord, ByteBuffer eventId)
    {
        for (Map.Entry<String, ByteBuffer> entry : rawPayload.entrySet())
        {
            cf.addColumn(new ExpiringColumn(buildName(CFMetaData.TraceEventsCf, coord, eventId, PAYLOAD_BB,
                    UTF8Type.instance.decompose(entry.getKey())), entry.getValue(), System.currentTimeMillis(),
                                            TTL));
        }
    }

    private void addPayloadTypeColumns(ColumnFamily cf,
            Map<String, AbstractType<?>> payloadTypes, ByteBuffer coord, ByteBuffer eventId)
    {

        for (Map.Entry<String, AbstractType<?>> entry : payloadTypes.entrySet())
        {
            cf.addColumn(new ExpiringColumn(buildName(CFMetaData.TraceEventsCf, coord, eventId, PAYLOAD_TYPES_BB,
                    UTF8Type.instance.decompose(entry.getKey())), UTF8Type.instance.decompose(entry.getValue()
                    .toString()), System.currentTimeMillis(), TTL));
        }

    }

    private ByteBuffer buildName(CFMetaData meta, ByteBuffer... args)
    {
        ColumnNameBuilder builder = meta.getCfDef().getColumnNameBuilder();
        for (ByteBuffer arg : args)
        {
            builder.add(arg);
        }
        return builder.build();
    }

    private ByteBuffer bytes(InetAddress address)
    {
        return ByteBuffer.wrap(address.getAddress());
    }

    /**
     * Copies the thread local state, if any. Used when the QueryContext needs to be copied into another thread. Use the
     * update() function to update the thread local state.
     */
    public TraceState copy()
    {
        final TraceState tls = state.get();
        return tls == null ? null : new TraceState(tls);
    }

    public UUID getSessionId()
    {
        return isTracing() ? state.get().sessionId : null;
    }

    /**
     * Indicates if the query originated on this node.
     */
    public boolean isLocalTraceSession()
    {
        final TraceState tls = state.get();
        return ((tls != null) && tls.coordinator.equals(localAddress)) ? true : false;
    }

    /**
     * Indicates if the current thread's execution is being traced.
     */
    public static boolean isTracing()
    {
        return instance != null && instance.state.get() != null;
    }

    public void reset()
    {
        state.set(null);
    }

    @VisibleForTesting
    public void setLocalAddress(InetAddress localAddress)
    {
        this.localAddress = localAddress;
    }

    public UUID newSession()
    {
        return newSession(TimeUUIDType.instance.compose(ByteBuffer.wrap(UUIDGen.getTimeUUIDBytes())));
    }

    public UUID newSession(UUID sessionId)
    {
        assert state.get() == null;

        TraceState ts = new TraceState(localAddress, sessionId);
        state.set(ts);

        return sessionId;
    }

    public void stopSession()
    {
        if (isTracing())
        {
            trace(TraceEvent.Type.SESSION_END.builder().build());
        }
        reset();
    }

    /**
     * Separated and made visible so that we can override the actual storage for testing purposes.
     */
    @VisibleForTesting
    protected void store(final UUID key, final ColumnFamily family)
    {
        try
        {
            StageManager.getStage(Stage.TRACING).execute(new Runnable()
            {
                public void run()
                {
                    RowMutation mutation = new RowMutation(TRACE_KS, TimeUUIDType.instance.decompose(key));
                    mutation.add(family);
                    try
                    {
                        StorageProxy.mutate(Arrays.asList(mutation), ConsistencyLevel.ANY);
                    }
                    catch (Exception e)
                    {
                        logger.error("Failed tracing row mutation. Key: " + key + " CF: " + family);
                    }
                }
            });
        }
        catch (RejectedExecutionException e)
        {
            logger.warn("Cannot trace event. Tracing queue is full. Key: " + key + " CF: " + family);
        }
    }

    public TraceState threadLocalState()
    {
        return state.get();
    }

    public UUID trace(TraceEvent event)
    {
        if (isTracing())
        {
            // log the event to debug (in case tracing queue is full)
            logger.debug("Tracing event: " + event);
            ColumnFamily family = ColumnFamily.create(CFMetaData.TraceEventsCf);
            ByteBuffer coordinatorAsBB = bytes(event.coordinator());
            ByteBuffer eventIdAsBB = event.idAsBB();
            addColumn(family, buildName(CFMetaData.TraceEventsCf, coordinatorAsBB, eventIdAsBB, SOURCE_BB), event.source());
            addColumn(family, buildName(CFMetaData.TraceEventsCf, coordinatorAsBB, eventIdAsBB, NAME_BB), event.name());
            addColumn(family, buildName(CFMetaData.TraceEventsCf, coordinatorAsBB, eventIdAsBB, DURATION_BB), event.duration());
            addColumn(family, buildName(CFMetaData.TraceEventsCf, coordinatorAsBB, eventIdAsBB, HAPPENED_BB), event.timestamp());
            addColumn(family, buildName(CFMetaData.TraceEventsCf, coordinatorAsBB, eventIdAsBB, DESCRIPTION_BB), event.description());
            addColumn(family, buildName(CFMetaData.TraceEventsCf, coordinatorAsBB, eventIdAsBB, TYPE_BB), event.type().name());
            addPayloadTypeColumns(family, event.payloadTypes(), coordinatorAsBB, eventIdAsBB);
            addPayloadColumns(family, event.rawPayload(), coordinatorAsBB, eventIdAsBB);
            store(event.sessionId(), family);
            return event.id();
        }
        return null;
    }

    /**
     * Updates the threads query context from a message
     * 
     * @param message
     *            The internode message
     */
    public void traceMessageArrival(final MessageIn<?> message, String id, String description)
    {
        final byte[] sessionBytes = message.parameters.get(TraceContext.TRACE_HEADER);

        // if the message has no session context header don't do tracing
        if (sessionBytes == null)
        {
            state.set(null);
            return;
        }

        checkState(sessionBytes.length == 16);
        state.set(new TraceState(message.from, UUIDGen.getUUID(ByteBuffer.wrap(sessionBytes))));

        trace(TraceEvent.Type.MESSAGE_ARRIVAL.builder().name("MessageArrival[" + id + "]").description(description).build());
    }

    /**
     * Updates the Query Context for this thread. Call copy() to obtain a copy of a threads query context.
     */
    public void update(final TraceState tls)
    {
        state.set(tls);
    }
}
