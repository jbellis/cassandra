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
package org.apache.cassandra.tracing;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.cassandra.tracing.TraceSessionContext.traceCtx;
import static org.apache.cassandra.tracing.TraceSessionContext.traceTableMetadata;

import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.TimeUnit;

import org.apache.cassandra.config.ConfigurationException;
import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.IColumn;
import org.apache.cassandra.db.marshal.*;
import org.apache.cassandra.thrift.ColumnOrSuperColumn;
import org.apache.cassandra.tracing.TraceEvent.Type;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.UUIDGen;

import com.google.common.base.Function;
import com.google.common.base.Supplier;
import com.google.common.collect.*;

import org.apache.thrift.TBase;
import org.apache.thrift.TEnum;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A fluent builder for trace events. Is also able to build events from raw columns (or thrift columns).
 */
public class TraceEventBuilder
{

    private static final Logger logger = LoggerFactory.getLogger(TraceSessionContext.class);

    public static List<TraceEvent> fromThrift(UUID sessionId,
            List<ColumnOrSuperColumn> columnOrSuperColumns)
    {
        return processRawValues(sessionId, Iterables.transform(columnOrSuperColumns,
                new Function<ColumnOrSuperColumn, Pair<ByteBuffer, ByteBuffer>>()
                {
                    public Pair<ByteBuffer, ByteBuffer> apply(ColumnOrSuperColumn column)
                    {
                        return new Pair<ByteBuffer, ByteBuffer>(column.column.name, column.column.value);
                    }
                }));
    }

    public static List<TraceEvent> fromColumnFamily(UUID key, ColumnFamily cf)
    {
        return processRawValues(key, Iterables.transform(cf, new Function<IColumn, Pair<ByteBuffer, ByteBuffer>>()
        {
            public Pair<ByteBuffer, ByteBuffer> apply(IColumn column)
            {
                return new Pair<ByteBuffer, ByteBuffer>(column.name(), column.value());
            }
        }));
    }

    public static InetAddress decodeCoordinator(ByteBuffer name)
    {
        CompositeType traceColumnType = ((CompositeType) traceTableMetadata().comparator);
        AbstractType<InetAddress> coordinatorType = (AbstractType<InetAddress>) traceColumnType.types.get(0);
        return coordinatorType.compose(traceColumnType.deconstruct(name).get(0).value);
    }

    public static UUID decodeEventId(ByteBuffer name)
    {
        CompositeType traceColumnType = ((CompositeType) traceTableMetadata().comparator);
        AbstractType<UUID> eventIdComponentType = (AbstractType<UUID>) traceColumnType.types.get(1);
        return eventIdComponentType.compose(traceColumnType.deconstruct(name).get(1).value);
    }

    public static String decodeColumnName(ByteBuffer name)
    {
        CompositeType traceColumnType = ((CompositeType) traceTableMetadata().comparator);
        AbstractType<String> columnNameType = (AbstractType<String>) traceColumnType.types.get(2);
        return columnNameType.compose(traceColumnType.deconstruct(name).get(2).value);
    }

    public static String decodeMapEntryKey(ByteBuffer name)
    {
        // here we cheat a bit as we now that both map types have a UTF8Type key (avoids having a method per map)
        CompositeType traceColumnType = ((CompositeType) traceTableMetadata().comparator);
        return UTF8Type.instance.compose(traceColumnType.deconstruct(name).get(3).value);
    }

    private static List<TraceEvent> processRawValues(UUID key, Iterable<Pair<ByteBuffer, ByteBuffer>> colNamesAndValues)
    {
        Multimap<UUID, Pair<ByteBuffer, ByteBuffer>> eventColumns = Multimaps.newListMultimap(
                Maps.<UUID, Collection<Pair<ByteBuffer, ByteBuffer>>> newLinkedHashMap(),
                new Supplier<ArrayList<Pair<ByteBuffer, ByteBuffer>>>()
                {
                    @Override
                    public ArrayList<Pair<ByteBuffer, ByteBuffer>> get()
                    {
                        return Lists.newArrayList();
                    }
                });

        // split the columns by event
        for (Pair<ByteBuffer, ByteBuffer> column : colNamesAndValues)
        {
            UUID eventId = decodeEventId(column.left);
            eventColumns.put(eventId, column);
        }

        List<TraceEvent> events = Lists.newArrayList();
        for (Map.Entry<UUID, Collection<Pair<ByteBuffer, ByteBuffer>>> entry : eventColumns.asMap().entrySet())
        {
            TraceEventBuilder builder = new TraceEventBuilder(entry.getKey());
            builder.sessionId(key);
            boolean setCoordinator = false;
            for (Pair<ByteBuffer, ByteBuffer> col : entry.getValue())
            {
                if (!setCoordinator)
                {
                    builder.coordinator(decodeCoordinator(col.left));
                    setCoordinator = true;
                }

                String colName = decodeColumnName(col.left);
                if (colName.equals(TraceSessionContext.DESCRIPTION))
                {
                    builder.description(UTF8Type.instance.compose(col.right));
                    continue;
                }
                if (colName.equals(TraceSessionContext.DURATION))
                {
                    builder.duration(LongType.instance.compose(col.right));
                    continue;
                }
                if (colName.equals(TraceSessionContext.HAPPENED))
                {
                    builder.timestamp(LongType.instance.compose(col.right));
                    continue;
                }
                if (colName.equals(TraceSessionContext.NAME))
                {
                    builder.name(UTF8Type.instance.compose(col.right));
                    continue;
                }
                if (colName.equals(TraceSessionContext.PAYLOAD))
                {
                    String payloadKey = decodeMapEntryKey(col.left);
                    builder.addPayloadRaw(payloadKey, col.right);
                    continue;
                }
                if (colName.equals(TraceSessionContext.PAYLOAD_TYPES))
                {
                    String payloadKey = decodeMapEntryKey(col.left);
                    try
                    {
                        builder.addPayloadTypeRaw(payloadKey,
                                new TypeParser(UTF8Type.instance.compose(col.right)).parse());
                    }
                    catch (ConfigurationException e)
                    {
                        logger.warn("error parsing payload type for payload key: " + payloadKey
                                + ", payload will have BytesType");
                        builder.addPayloadTypeRaw(payloadKey, BytesType.instance);
                    }
                    continue;
                }
                if (colName.equals(TraceSessionContext.SOURCE))
                {
                    builder.source(InetAddressType.instance.compose(col.right));
                    continue;
                }
                if (colName.equals(TraceSessionContext.TYPE))
                {
                    builder.type(Type.valueOf(UTF8Type.instance.compose(col.right)));
                    continue;
                }
            }
            events.add(builder.build());
        }
        return events;
    }

    private UUID sessionId;
    private UUID eventId;
    private String name;
    private String description;
    private Long duration;
    private Long timestamp;
    private InetAddress coordinator;
    private InetAddress source;
    private Map<String, AbstractType<?>> payloadTypes = Maps.newHashMap();
    private Map<String, ByteBuffer> payload = Maps.newHashMap();
    private TraceEvent.Type type;

    private TraceEventBuilder(UUID eventId)
    {
        this.eventId = eventId;
    }

    public TraceEventBuilder()
    {
    }

    public TraceEventBuilder sessionId(byte[] sessionId)
    {
        // check if isTracing so that we can have noop when not tracing (avoids having to do isTracing checks everywhere
        // on traced code)
        if (isTracing())
            this.sessionId = UUIDType.instance.compose(ByteBuffer.wrap(sessionId));
        return this;
    }

    public TraceEventBuilder sessionId(UUID sessionId)
    {
        if (isTracing())
            this.sessionId = sessionId;
        return this;
    }

    public TraceEventBuilder name(String name)
    {
        if (isTracing())
            this.name = name;
        return this;
    }

    public TraceEventBuilder description(String description)
    {
        if (isTracing())
            this.description = description;
        return this;
    }

    public TraceEventBuilder duration(long duration)
    {
        if (isTracing())
            this.duration = duration;
        return this;
    }

    public TraceEventBuilder timestamp(long timestamp)
    {
        if (isTracing())
            this.timestamp = timestamp;
        return this;
    }

    public TraceEventBuilder eventId(byte[] eventId)
    {
        if (isTracing())
            eventId(UUIDType.instance.compose(ByteBuffer.wrap(eventId)));
        return this;
    }

    public TraceEventBuilder eventId(UUID eventId)
    {
        if (isTracing())
            this.eventId = eventId;
        return this;
    }

    public TraceEventBuilder coordinator(InetAddress coordinator)
    {
        if (isTracing())
            this.coordinator = coordinator;
        return this;
    }

    public TraceEventBuilder source(InetAddress source)
    {
        if (isTracing())
            this.source = source;
        return this;
    }

    public TraceEventBuilder type(TraceEvent.Type type)
    {
        if (isTracing())
            this.type = type;
        return this;
    }

    public <T> TraceEventBuilder addPayload(String name, AbstractType<?> type, T value)
    {
        if (isTracing())
        {
            @SuppressWarnings("unchecked")
            ByteBuffer encoded = ((AbstractType<T>) type).decompose(value);
            this.payloadTypes.put(name, type);
            this.payload.put(name, encoded);
        }
        return this;
    }

    public TraceEventBuilder addPayload(String name, TEnum thriftEnum)
    {
        if (isTracing())
        {
            ByteBuffer encoded = Int32Type.instance.decompose(thriftEnum.getValue());
            this.payloadTypes.put(name, Int32Type.instance);
            this.payload.put(name, encoded);
        }
        return this;
    }

    public TraceEventBuilder addPayload(String name, TBase<?, ?> thriftObject)
    {
        if (isTracing())
        {
            ThriftType type = ThriftType.getInstance(thriftObject.getClass());
            this.payloadTypes.put(name, type);
            this.payload.put(name, type.decompose(thriftObject));
        }
        return this;
    }

    public TraceEventBuilder addPayload(String name, ByteBuffer byteBuffer)
    {
        if (isTracing())
        {
            this.payloadTypes.put(name, BytesType.instance);
            this.payload.put(name, byteBuffer);
        }
        return this;
    }

    public TraceEventBuilder addPayload(String name, int value)
    {
        if (isTracing())
        {
            this.payloadTypes.put(name, Int32Type.instance);
            this.payload.put(name, Int32Type.instance.decompose(value));
        }
        return this;
    }

    public TraceEventBuilder addPayload(String name, long value)
    {
        if (isTracing())
        {
            this.payloadTypes.put(name, LongType.instance);
            this.payload.put(name, LongType.instance.decompose(value));
        }
        return this;
    }

    public TraceEventBuilder addPayload(String name, boolean value)
    {
        if (isTracing())
        {
            this.payloadTypes.put(name, BooleanType.instance);
            this.payload.put(name, BooleanType.instance.decompose(value));
        }
        return this;
    }

    public TraceEventBuilder addPayload(String name, String value)
    {
        if (isTracing())
        {
            this.payloadTypes.put(name, UTF8Type.instance);
            this.payload.put(name, UTF8Type.instance.decompose(value));
        }
        return this;
    }

    public <T> TraceEventBuilder addPayload(String name, AbstractType<T> componentsType, List<T> componentList)
    {
        if (isTracing())
        {
            ListType<T> listType = ListType.getInstance(componentsType);
            this.payloadTypes.put(name, listType);
            this.payload.put(name, listType.decompose(componentList));
        }
        return this;
    }

    /**
     * Adds a payload without inserting type (which might come later). Used internally for deserialization.
     */
    private TraceEventBuilder addPayloadRaw(String name, ByteBuffer byteBuffer)
    {
        if (isTracing())
            this.payload.put(name, byteBuffer);
        return this;
    }

    private TraceEventBuilder addPayloadTypeRaw(String name, AbstractType<?> type)
    {
        if (isTracing())
            this.payloadTypes.put(name, type);
        return this;
    }

    public TraceEvent build()
    {
        if (isTracing())
        {
            if (coordinator == null)
            {
                coordinator = traceCtx().threadLocalState().origin;
                checkNotNull(coordinator,
                        "coordinator must be provided or be set at the current thread's TraceSessionContextThreadLocalState");
            }
            if (source == null)
            {
                source = traceCtx().threadLocalState().source;
                checkNotNull(source,
                        "source must be provided or be set at the current thread's TraceSessionContextThreadLocalState");
            }
            if (eventId == null)
            {
                eventId(UUIDGen.getTimeUUIDBytes());
            }

            if (type == null)
            {
                type = TraceEvent.Type.MISC;
            }

            if (name == null)
            {
                name = type.name();
            }
            if (description == null)
            {
                description = "";
            }
            if (sessionId == null)
            {
                sessionId = traceCtx().threadLocalState().sessionId;
                checkNotNull(sessionId,
                        "sessionId must be provided or be set at the current thread's TraceSessionContextThreadLocalState");
            }
            if (duration == null)
            {
                duration = traceCtx().threadLocalState().watch.elapsedTime(TimeUnit.NANOSECONDS);
                checkNotNull(duration,
                        "duration must be provided or be measured from the current thread's TraceSessionContextThreadLocalState");

            }
            if (timestamp == null)
            {
                timestamp = System.currentTimeMillis();
            }

            return new TraceEvent(name, description, duration, timestamp, sessionId, eventId, coordinator, source,
                    type,
                    payload, payloadTypes);
        }
        return null;
    }

    private boolean isTracing()
    {
        return eventId != null ? true : TraceSessionContext.isTracing();
    }

}
