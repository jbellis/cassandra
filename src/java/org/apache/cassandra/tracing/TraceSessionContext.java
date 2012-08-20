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
import org.apache.cassandra.config.ConfigurationException;
import org.apache.cassandra.config.KSMetaData;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.cql3.ColumnNameBuilder;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.statements.CreateColumnFamilyStatement;
import org.apache.cassandra.cql3.statements.CreateIndexStatement;
import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.ColumnFamilyType;
import org.apache.cassandra.db.ExpiringColumn;
import org.apache.cassandra.db.RowMutation;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.TimeUUIDType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.locator.SimpleStrategy;
import org.apache.cassandra.net.MessageIn;
import org.apache.cassandra.net.MessageOut;
import org.apache.cassandra.service.MigrationManager;
import org.apache.cassandra.service.StorageProxy;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.UUIDGen;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Predicate;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A trace session context. Able to track and store trace sessions. A session is usually a user initiated query, and may
 * have multiple local and remote events before it is completed. All events and sessions are stored at table.
 */
public class TraceSessionContext
{

    private static TraceSessionContext ctx;

    public static final String COORDINATOR = "coordinator";
    public static final String DESCRIPTION = "description";
    public static final ByteBuffer DESCRIPTION_BB = ByteBufferUtil.bytes(DESCRIPTION);
    public static final String DURATION = "duration";
    public static final ByteBuffer DURATION_BB = ByteBufferUtil.bytes(DURATION);
    public static final String EVENT_ID = "eventId";
    public static final String EVENTS_TABLE = "trace_events";
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
    public static final String TRACE_KEYSPACE = "trace";
    public static final String TRACE_SESSION_CONTEXT_HEADER = "SessionContext";
    public static final String TYPE = "type";
    public static final ByteBuffer TYPE_BB = ByteBufferUtil.bytes(TYPE);

    public static final String TRACE_TABLE_STATEMENT = "CREATE TABLE " + TRACE_KEYSPACE + "." + EVENTS_TABLE + " (" +
            "  " + SESSION_ID + "        timeuuid," +
            "  " + COORDINATOR + "       inet," +
            "  " + EVENT_ID + "          timeuuid," +
            "  " + DESCRIPTION + "       text," +
            "  " + DURATION + "          bigint," +
            "  " + HAPPENED + "          timestamp," +
            "  " + NAME + "              text," +
            "  " + PAYLOAD_TYPES + "     map<text, text>," +
            "  " + PAYLOAD + "           map<text, blob>," +
            "  " + SOURCE + "            inet," +
            "  " + TYPE + "              text," +
            "  PRIMARY KEY (" + SESSION_ID + ", " + COORDINATOR + ", " + EVENT_ID + "));";

    public static final String INDEX_STATEMENT = "CREATE INDEX idx_" + NAME + " ON " + TRACE_KEYSPACE + "."
            + EVENTS_TABLE + " (" + NAME + ")";

    private static final CFMetaData eventsCfm = compile(TRACE_TABLE_STATEMENT);

    private static final Logger logger = LoggerFactory.getLogger(TraceSessionContext.class);

    private static CFMetaData compile(String cql)
    {
        CreateColumnFamilyStatement statement = null;
        try
        {
            statement = (CreateColumnFamilyStatement) QueryProcessor.parseStatement(cql)
                    .prepare().statement;

            CFMetaData newCFMD = new CFMetaData(TRACE_KEYSPACE, statement.columnFamily(), ColumnFamilyType.Standard,
                    statement.comparator,
                    null);

            newCFMD.comment("")
                    .readRepairChance(0)
                    .dcLocalReadRepairChance(0)
                    .gcGraceSeconds(0);

            statement.applyPropertiesTo(newCFMD);

            return newCFMD;
        }
        catch (InvalidRequestException e)
        {
            throw Throwables.propagate(e);
        }
        catch (ConfigurationException e)
        {
            throw Throwables.propagate(e);
        }
    }

    public static void initialize()
    {
        try
        {
            ctx = new TraceSessionContext();
            logger.info("Tracing system enabled and initialized.");
        }
        catch (Exception e)
        {
            logger.error("Error initializing tracing system. Tracing will be disabled", e);
        }
    }

    @VisibleForTesting
    public static void setCtx(TraceSessionContext context)
    {
        ctx = context;
    }

    /**
     * Fetches and lazy initializes the trace context.
     */
    public static TraceSessionContext traceCtx()
    {
        return ctx;
    }

    public static CFMetaData traceTableMetadata()
    {
        return eventsCfm;
    }

    private InetAddress localAddress;
    private ThreadLocal<TraceSessionContextThreadLocalState> sessionContextThreadLocalState = new ThreadLocal<TraceSessionContextThreadLocalState>();
    private int timeToLive = 86400;

    protected TraceSessionContext()
    {
        logger.info("Initializing Trace session context.");
        if (!Iterables.tryFind(Schema.instance.getTables(), new Predicate<String>()
        {
            public boolean apply(String keyspace)
            {
                return keyspace.equals(TRACE_KEYSPACE);
            }

        }).isPresent())
        {
            try
            {
                logger.info("Trace keyspace was not found creating & announcing");
                KSMetaData traceKs = KSMetaData.newKeyspace(TRACE_KEYSPACE, SimpleStrategy.class.getName(),
                        ImmutableMap.of("replication_factor", "1"));
                MigrationManager.announceNewKeyspace(traceKs);
                MigrationManager.announceNewColumnFamily(eventsCfm);
                Thread.sleep(1000);
                try
                {
                    CreateIndexStatement statement = (CreateIndexStatement) QueryProcessor
                            .parseStatement(INDEX_STATEMENT).prepare().statement;
                    statement.announceMigration();
                }
                catch (InvalidRequestException e)
                {
                    if (!e.getWhy().contains("Index already exists"))
                    {
                        Throwables.propagate(e);
                    }
                }
            }
            catch (Exception e)
            {
                Throwables.propagate(e);
            }
        }

        this.localAddress = FBUtilities.getLocalAddress();
    }

    private void addColumn(ColumnFamily cf, ByteBuffer columnName, InetAddress address)
    {
        cf.addColumn(new ExpiringColumn(columnName, bytes(address), System.currentTimeMillis(), timeToLive));
    }

    private void addColumn(ColumnFamily cf, ByteBuffer columnName, long value)
    {
        cf.addColumn(new ExpiringColumn(columnName, ByteBufferUtil.bytes(value), System.currentTimeMillis(), timeToLive));
    }

    private void addColumn(ColumnFamily cf, ByteBuffer columnName, String value)
    {
        cf.addColumn(new ExpiringColumn(columnName, ByteBufferUtil.bytes(value), System.currentTimeMillis(), timeToLive));
    }

    private void addPayloadColumns(ColumnFamily cf, Map<String, ByteBuffer> rawPayload,
            ByteBuffer coord, ByteBuffer eventId)
    {
        for (Map.Entry<String, ByteBuffer> entry : rawPayload.entrySet())
        {
            cf.addColumn(new ExpiringColumn(buildName(eventsCfm, coord, eventId, PAYLOAD_BB,
                    UTF8Type.instance.decompose(entry.getKey())), entry.getValue(), System.currentTimeMillis(),
                    timeToLive));
        }
    }

    private void addPayloadTypeColumns(ColumnFamily cf,
            Map<String, AbstractType<?>> payloadTypes, ByteBuffer coord, ByteBuffer eventId)
    {

        for (Map.Entry<String, AbstractType<?>> entry : payloadTypes.entrySet())
        {
            cf.addColumn(new ExpiringColumn(buildName(eventsCfm, coord, eventId, PAYLOAD_TYPES_BB,
                    UTF8Type.instance.decompose(entry.getKey())), UTF8Type.instance.decompose(entry.getValue()
                    .toString()), System.currentTimeMillis(), timeToLive));
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
    public TraceSessionContextThreadLocalState copy()
    {
        final TraceSessionContextThreadLocalState tls = sessionContextThreadLocalState.get();
        return tls == null ? null : new TraceSessionContextThreadLocalState(tls);
    }

    /**
     * Creates a byte[] to use a message header to serialize this context to another node, if any. The context is only
     * included in the message if it started locally.
     * 
     * @return
     */
    public byte[] getSessionContextHeader()
    {
        if (!isLocalTraceSession())
            return null;

        // this uses a FBA so no need to close()
        @SuppressWarnings("resource")
        final DataOutputBuffer buffer = new DataOutputBuffer();
        try
        {
            byte[] sessionId = TimeUUIDType.instance.decompose(getSessionId()).array();
            buffer.writeInt(sessionId.length);
            buffer.write(sessionId);
            return buffer.getData();
        }
        catch (IOException e)
        {
            throw new IOError(e);
        }
    }

    public UUID getSessionId()
    {
        return isTracing() ? sessionContextThreadLocalState.get().sessionId : null;
    }

    /**
     * Indicates if the query originated on this node.
     */
    public boolean isLocalTraceSession()
    {
        final TraceSessionContextThreadLocalState tls = sessionContextThreadLocalState.get();
        return ((tls != null) && tls.origin.equals(localAddress)) ? true : false;
    }

    /**
     * Indicates if the current thread's execution is being traced.
     */
    public static boolean isTracing()
    {
        return ctx != null && ctx.sessionContextThreadLocalState.get() != null;
    }

    public void reset()
    {
        sessionContextThreadLocalState.set(null);
    }

    @VisibleForTesting
    public void setLocalAddress(InetAddress localAddress)
    {
        this.localAddress = localAddress;
    }

    public void setTimeToLive(int timeToLive)
    {
        this.timeToLive = timeToLive;
    }

    public UUID newSession()
    {
        return newSession(TimeUUIDType.instance.compose(ByteBuffer.wrap(UUIDGen.getTimeUUIDBytes())));
    }

    public UUID newSession(UUID sessionId)
    {
        assert sessionContextThreadLocalState.get() == null;

        TraceSessionContextThreadLocalState tsctls = new TraceSessionContextThreadLocalState(localAddress,
                localAddress, sessionId);

        sessionContextThreadLocalState.set(tsctls);

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
                    RowMutation mutation = new RowMutation(TRACE_KEYSPACE, TimeUUIDType.instance.decompose(key));
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

    public TraceSessionContextThreadLocalState threadLocalState()
    {
        return sessionContextThreadLocalState.get();
    }

    public UUID trace(TraceEvent event)
    {
        if (isTracing())
        {
            // log the event to debug (in case tracing queue is full)
            logger.debug("Tracing event: " + event);
            ColumnFamily family = ColumnFamily.create(eventsCfm);
            ByteBuffer coordinatorAsBB = bytes(event.coordinator());
            ByteBuffer eventIdAsBB = event.idAsBB();
            addColumn(family, buildName(eventsCfm, coordinatorAsBB, eventIdAsBB, SOURCE_BB), event.source());
            addColumn(family, buildName(eventsCfm, coordinatorAsBB, eventIdAsBB, NAME_BB), event.name());
            addColumn(family, buildName(eventsCfm, coordinatorAsBB, eventIdAsBB, DURATION_BB), event.duration());
            addColumn(family, buildName(eventsCfm, coordinatorAsBB, eventIdAsBB, HAPPENED_BB), event.timestamp());
            addColumn(family, buildName(eventsCfm, coordinatorAsBB, eventIdAsBB, DESCRIPTION_BB), event.description());
            addColumn(family, buildName(eventsCfm, coordinatorAsBB, eventIdAsBB, TYPE_BB), event.type().name());
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
        final byte[] queryContextBytes = message.parameters
                .get(TraceSessionContext.TRACE_SESSION_CONTEXT_HEADER);

        // if the message has no session context header don't do tracing
        if (queryContextBytes == null)
            return;

        checkState(queryContextBytes.length > 0);
        final DataInputStream dis = new DataInputStream(new ByteArrayInputStream(queryContextBytes));
        final byte[] sessionId;
        try
        {
            sessionId = new byte[dis.readInt()];
            dis.read(sessionId);
        }
        catch (IOException e)
        {
            throw new IOError(e);
        }
        sessionContextThreadLocalState.set(new TraceSessionContextThreadLocalState(message.from, localAddress,
                TimeUUIDType.instance.compose(ByteBuffer.wrap(sessionId)), id));

        trace(TraceEvent.Type.MESSAGE_ARRIVAL.builder().name("MessageArrival[" + id + "]")
                .description(description).build());
    }

    public MessageOut traceMessageDeparture(MessageOut<?> messageOut, String id, String description)
    {
        byte[] tracePayload = traceCtx().getSessionContextHeader();
        if (tracePayload != null)
        {
            messageOut = messageOut.withParameter(TRACE_SESSION_CONTEXT_HEADER, tracePayload);
            trace(TraceEvent.Type.MESSAGE_DEPARTURE.builder().name("MessageDeparture[" + id + "]")
                    .description(description).build());
        }

        return messageOut;
    }

    /**
     * Updates the Query Context for this thread. Call copy() to obtain a copy of a threads query context.
     */
    public void update(final TraceSessionContextThreadLocalState tls)
    {
        sessionContextThreadLocalState.set(tls);
    }
}
