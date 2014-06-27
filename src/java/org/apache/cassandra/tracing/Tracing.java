/*
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

import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.concurrent.Stage;
import org.apache.cassandra.concurrent.StageManager;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.composites.CellName;
import org.apache.cassandra.db.marshal.TimeUUIDType;
import org.apache.cassandra.exceptions.OverloadedException;
import org.apache.cassandra.exceptions.UnavailableException;
import org.apache.cassandra.exceptions.WriteTimeoutException;
import org.apache.cassandra.net.MessageIn;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.service.StorageProxy;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.UUIDGen;

import static org.apache.cassandra.utils.ByteBufferUtil.bytes;

/**
 * A trace session context. Able to track and store trace sessions. A session is usually a user initiated query, and may
 * have multiple local and remote events before it is completed. All events and sessions are stored at keyspace.
 */
public class Tracing
{
    public static final String TRACE_KS = "system_traces";
    public static final String EVENTS_CF = "events";
    public static final String SESSIONS_CF = "sessions";
    public static final String TRACE_HEADER = "TraceSession";
    public static final String TRACE_TYPE = "TraceType";
    public static final String TRACE_TTL = "TraceTTL";

    private static final int MIN_SHIFT = 4;

    public enum TraceType
    {
        QUERY,
        REPAIR;

        private static final TraceType[] ALL_VALUES = values();

        public static TraceType deserialize(byte b)
        {
            return ALL_VALUES[b];
        }

        public static byte serialize(TraceType value)
        {
            return (byte) value.ordinal();
        }

        private static final int[] TTLS = { DatabaseDescriptor.getTracetypeQueryTTL(),
                                            DatabaseDescriptor.getTracetypeRepairTTL() };

        public int getTTL()
        {
            return TTLS[ordinal()];
        }
    }

    public static final long TRACETYPE_REPAIR = 1L << (MIN_SHIFT + 1);

    private static final Logger logger = LoggerFactory.getLogger(Tracing.class);

    private final InetAddress localAddress = FBUtilities.getLocalAddress();

    private final ThreadLocal<TraceState> state = new ThreadLocal<TraceState>();

    private final ConcurrentMap<UUID, TraceState> sessions = new ConcurrentHashMap<UUID, TraceState>();

    public static final Tracing instance = new Tracing();

    public static void addColumn(ColumnFamily cf, CellName name, InetAddress address, int ttl)
    {
        addColumn(cf, name, ByteBufferUtil.bytes(address), ttl);
    }

    public static void addColumn(ColumnFamily cf, CellName name, int value, int ttl)
    {
        addColumn(cf, name, ByteBufferUtil.bytes(value), ttl);
    }

    public static void addColumn(ColumnFamily cf, CellName name, long value, int ttl)
    {
        addColumn(cf, name, ByteBufferUtil.bytes(value), ttl);
    }

    public static void addColumn(ColumnFamily cf, CellName name, String value, int ttl)
    {
        addColumn(cf, name, ByteBufferUtil.bytes(value), ttl);
    }

    private static void addColumn(ColumnFamily cf, CellName name, ByteBuffer value, int ttl)
    {
        cf.addColumn(new BufferExpiringCell(name, value, System.currentTimeMillis(), ttl));
    }

    public void addParameterColumns(ColumnFamily cf, Map<String, String> rawPayload, int ttl)
    {
        for (Map.Entry<String, String> entry : rawPayload.entrySet())
        {
            cf.addColumn(new BufferExpiringCell(buildName(CFMetaData.TraceSessionsCf, "parameters", entry.getKey()),
                                                bytes(entry.getValue()), System.currentTimeMillis(), ttl));
        }
    }

    public static CellName buildName(CFMetaData meta, Object... args)
    {
        return meta.comparator.makeCellName(args);
    }

    public UUID getSessionId()
    {
        assert isTracing();
        return state.get().sessionId;
    }

    public TraceType getTraceType()
    {
        assert isTracing();
        return state.get().traceType;
    }

    public int getTTL()
    {
        assert isTracing();
        return state.get().ttl;
    }

    /**
     * Indicates if the current thread's execution is being traced.
     */
    public static boolean isTracing()
    {
        return instance.state.get() != null;
    }

    public UUID newSession()
    {
        return newSession(TraceType.QUERY);
    }

    public UUID newSession(TraceType traceType)
    {
        return newSession(TimeUUIDType.instance.compose(ByteBuffer.wrap(UUIDGen.getTimeUUIDBytes())), traceType);
    }

    public UUID newSession(UUID sessionId)
    {
        return newSession(sessionId, TraceType.QUERY);
    }

    public UUID newSession(UUID sessionId, TraceType traceType)
    {
        assert state.get() == null;

        TraceState ts = new TraceState(localAddress, sessionId, traceType);
        state.set(ts);
        sessions.put(sessionId, ts);

        return sessionId;
    }

    public void stopNonLocal(TraceState state)
    {
        sessions.remove(state.sessionId);
    }

    /**
     * Stop the session and record its complete.  Called by coodinator when request is complete.
     */
    public void stopSession()
    {
        TraceState state = this.state.get();
        if (state == null) // inline isTracing to avoid implicit two calls to state.get()
        {
            logger.debug("request complete");
        }
        else
        {
            final int elapsed = state.elapsed();
            final ByteBuffer sessionIdBytes = state.sessionIdBytes;
            final int ttl = state.ttl;

            StageManager.getStage(Stage.TRACING).execute(new Runnable()
            {
                public void run()
                {
                    CFMetaData cfMeta = CFMetaData.TraceSessionsCf;
                    ColumnFamily cf = ArrayBackedSortedColumns.factory.create(cfMeta);
                    addColumn(cf, buildName(cfMeta, "duration"), elapsed, ttl);
                    mutateWithCatch(new Mutation(TRACE_KS, sessionIdBytes, cf));
                }
            });

            state.stop();
            sessions.remove(state.sessionId);
            this.state.set(null);
        }
    }

    public TraceState get()
    {
        return state.get();
    }

    public TraceState get(UUID sessionId)
    {
        return sessions.get(sessionId);
    }

    public void set(final TraceState tls)
    {
        state.set(tls);
    }

    public TraceState begin(final String request, final Map<String, String> parameters)
    {
        assert isTracing();

        final TraceState state = this.state.get();
        final long started_at = System.currentTimeMillis();
        final ByteBuffer sessionIdBytes = state.sessionIdBytes;
        final String command = state.traceType.toString();
        final int ttl = state.ttl;

        StageManager.getStage(Stage.TRACING).execute(new Runnable()
        {
            public void run()
            {
                CFMetaData cfMeta = CFMetaData.TraceSessionsCf;
                ColumnFamily cf = ArrayBackedSortedColumns.factory.create(cfMeta);
                addColumn(cf, buildName(cfMeta, "coordinator"), FBUtilities.getBroadcastAddress(), ttl);
                addParameterColumns(cf, parameters, ttl);
                addColumn(cf, buildName(cfMeta, bytes("request")), request, ttl);
                addColumn(cf, buildName(cfMeta, bytes("started_at")), started_at, ttl);
                addColumn(cf, buildName(cfMeta, bytes("command")), command, ttl);
                addParameterColumns(cf, parameters, ttl);
                mutateWithCatch(new Mutation(TRACE_KS, sessionIdBytes, cf));
            }
        });

        return state;
    }

    /**
     * Determines the tracing context from a message.  Does NOT set the threadlocal state.
     * 
     * @param message The internode message
     */
    public TraceState initializeFromMessage(final MessageIn<?> message)
    {
        final byte[] sessionBytes = message.parameters.get(TRACE_HEADER);

        if (sessionBytes == null)
            return null;

        assert sessionBytes.length == 16;
        UUID sessionId = UUIDGen.getUUID(ByteBuffer.wrap(sessionBytes));
        TraceState ts = sessions.get(sessionId);
        if (ts != null)
            return ts;

        byte[] tmpBytes;
        TraceType traceType = TraceType.QUERY;
        if ((tmpBytes = message.parameters.get(TRACE_TYPE)) != null)
            traceType = TraceType.deserialize(tmpBytes[0]);

        int ttl = traceType.getTTL();
        if ((tmpBytes = message.parameters.get(TRACE_TTL)) != null)
            ttl = ByteBuffer.wrap(tmpBytes).getInt();

        if (message.verb == MessagingService.Verb.REQUEST_RESPONSE)
        {
            // received a message for a session we've already closed out.  see CASSANDRA-5668
            return new ExpiredTraceState(sessionId, traceType, ttl);
        }
        else
        {
            ts = new TraceState(message.from, sessionId, traceType, ttl);
            sessions.put(sessionId, ts);
            return ts;
        }
    }


    // repair just gets a varargs method since it's so heavyweight anyway
    public static void traceRepair(String format, Object... args)
    {
        final TraceState state = instance.get();
        if (state == null) // inline isTracing to avoid implicit two calls to state.get()
            return;

        state.trace(format, args);
    }

    // normal traces get zero-, one-, and two-argument overloads so common case doesn't need to create varargs array
    public static void trace(String message)
    {
        final TraceState state = instance.get();
        if (state == null) // inline isTracing to avoid implicit two calls to state.get()
            return;

        state.trace(message);
    }

    public static void trace(String format, Object arg)
    {
        final TraceState state = instance.get();
        if (state == null) // inline isTracing to avoid implicit two calls to state.get()
            return;

        state.trace(format, arg);
    }

    public static void trace(String format, Object arg1, Object arg2)
    {
        final TraceState state = instance.get();
        if (state == null) // inline isTracing to avoid implicit two calls to state.get()
            return;

        state.trace(format, arg1, arg2);
    }

    public static void trace(String format, Object[] args)
    {
        final TraceState state = instance.get();
        if (state == null) // inline isTracing to avoid implicit two calls to state.get()
            return;

        state.trace(format, args);
    }

    static void mutateWithCatch(Mutation mutation)
    {
        try
        {
            StorageProxy.mutate(Arrays.asList(mutation), ConsistencyLevel.ANY);
        }
        catch (UnavailableException | WriteTimeoutException e)
        {
            // should never happen; ANY does not throw UAE or WTE
            throw new AssertionError(e);
        }
        catch (OverloadedException e)
        {
            logger.warn("Too many nodes are overloaded to save trace events");
        }
    }
}
