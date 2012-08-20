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

import static junit.framework.Assert.*;
import static org.apache.cassandra.tracing.TraceSessionContext.*;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.Util;
import org.apache.cassandra.concurrent.DebuggableThreadPoolExecutor;
import org.apache.cassandra.concurrent.Stage;
import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.RowMutation;
import org.apache.cassandra.db.Table;
import org.apache.cassandra.db.filter.QueryFilter;
import org.apache.cassandra.db.filter.QueryPath;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.db.marshal.TimeUUIDType;
import org.apache.cassandra.net.IVerbHandler;
import org.apache.cassandra.net.MessageIn;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.MessagingService.Verb;
import org.apache.cassandra.tracing.TraceEvent.Type;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.UUIDGen;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;

import org.junit.BeforeClass;
import org.junit.Test;

public class TraceSessionContextTest extends SchemaLoader
{

    private static class LocalTraceSessionContext extends TraceSessionContext
    {

        /**
         * Override the parent mutation that applies mutation to the cluster to instead apply mutations locally and
         * immediately for testing.
         */
        @Override
        protected void store(final UUID key, final ColumnFamily family)
        {
            RowMutation mutation = new RowMutation(TRACE_KEYSPACE, TimeUUIDType.instance.decompose(key));
            mutation.add(family);
            mutation.apply();
        }
    }

    private static UUID sessionId;

    @BeforeClass
    public static void loadSchema() throws IOException
    {
        SchemaLoader.loadSchema();
        TraceSessionContext.setCtx(new LocalTraceSessionContext());
    }

    @Test
    public void testNewSession() throws CharacterCodingException
    {
        sessionId = traceCtx().newSession();
        assertTrue(isTracing());
        assertTrue(traceCtx().isLocalTraceSession());
        assertNotNull(traceCtx().threadLocalState());

        // simulate a thrift req such as multiget_slice
        UUID eventId = traceCtx().trace(new TraceEventBuilder()
                .name("multiget_slice")
                .type(Type.SESSION_START)
                .build());

        ColumnFamily family = Table
                .open(TRACE_KEYSPACE)
                .getColumnFamilyStore(EVENTS_TABLE)
                .getColumnFamily(
                        QueryFilter.getIdentityFilter(Util.dk(ByteBuffer.wrap(UUIDGen.decompose(sessionId))),
                                new QueryPath(
                                        EVENTS_TABLE)));

        List<TraceEvent> traceEvents = TraceEventBuilder.fromColumnFamily(sessionId, family);
        // we should have two events because "get" actually produces one
        assertSame(1, traceEvents.size());

        TraceEvent event = Iterables.get(traceEvents, 0);
        assertEquals("multiget_slice", event.name());
        assertEquals(eventId, event.id());
    }

    @Test
    public void testNewLocalTraceEvent() throws CharacterCodingException, UnknownHostException
    {
        UUID eventId = traceCtx().trace(
                new TraceEventBuilder().name("simple trace event").duration(4321L).timestamp(1234L)
                        .addPayload("simplePayload", LongType.instance, 9876L).build());

        ColumnFamily family = Table
                .open(TRACE_KEYSPACE)
                .getColumnFamilyStore(EVENTS_TABLE)
                .getColumnFamily(
                        QueryFilter.getIdentityFilter(Util.dk(ByteBuffer.wrap(UUIDGen.decompose(sessionId))),
                                new QueryPath(
                                        EVENTS_TABLE)));

        List<TraceEvent> traceEvents = TraceEventBuilder.fromColumnFamily(sessionId, family);
        // we should have two events because "get" actually produces one
        assertSame(3, traceEvents.size());

        TraceEvent event = Iterables.get(traceEvents, 2);
        assertEquals("simple trace event", event.name());
        assertEquals(4321L, event.duration());
        assertEquals(1234L, event.timestamp());
        assertEquals(9876L, event.getFromPayload("simplePayload"));
        assertEquals(eventId, event.id());

    }

    @Test
    public void testContextTLStateAcompaniesToAnotherThread() throws InterruptedException, ExecutionException,
            CharacterCodingException, UnknownHostException
    {
        // the state should be carried to another thread as long as the debuggable TPE is used
        DebuggableThreadPoolExecutor poolExecutor = DebuggableThreadPoolExecutor
                .createWithFixedPoolSize("TestStage", 1);

        assertNotNull(poolExecutor.submit(new Callable<UUID>()
        {
            @Override
            public UUID call()
            {
                assertTrue(isTracing());
                assertEquals(sessionId, traceCtx().getSessionId());
                return traceCtx().trace(
                        new TraceEventBuilder().name("multi threaded trace event").duration(8765L).timestamp(5678L)
                                .build());
            }
        }).get());

        // The DebuggableTPE that will execute the task will insert a trace event AFTER
        // it returns so we need to wait a bit.
        Thread.sleep(200);

        ColumnFamily family = Table
                .open(TRACE_KEYSPACE)
                .getColumnFamilyStore(EVENTS_TABLE)
                .getColumnFamily(
                        QueryFilter.getIdentityFilter(Util.dk(ByteBuffer.wrap(UUIDGen.decompose(sessionId))),
                                new QueryPath(
                                        EVENTS_TABLE)));

        List<TraceEvent> traceEvents = TraceEventBuilder.fromColumnFamily(sessionId, family);

        assertSame(7, traceEvents.size());

        TraceEvent stageStartEvent = Iterables.get(traceEvents, 4);
        TraceEvent customEvent = Iterables.get(traceEvents, 5);
        TraceEvent stageFinishEvent = Iterables.get(traceEvents, 6);

        assertEquals("TestStage", stageStartEvent.name());
        assertSame(Type.STAGE_START, stageStartEvent.type());
        assertEquals(8765L, customEvent.duration());
        assertEquals("multi threaded trace event", customEvent.name());
        assertEquals(5678L, customEvent.timestamp());
        assertEquals(FBUtilities.getLocalAddress(), customEvent.source());
        assertEquals("TestStage", stageFinishEvent.name());
        assertSame(Type.STAGE_FINISH, stageFinishEvent.type());
    }

    @Test
    public void testTracingSessionsContinuesRemotelyIfMessageHasSessionContextHeader() throws UnknownHostException,
            InterruptedException, ExecutionException, CharacterCodingException
    {

        MessagingService.verbStages.put(Verb.UNUSED_1, Stage.MUTATION);

        MessageIn<Void> messageIn = MessageIn.create(FBUtilities.getLocalAddress(), null,
                ImmutableMap.<String, byte[]>
                        of(TraceSessionContext.TRACE_SESSION_CONTEXT_HEADER, traceCtx().getSessionContextHeader()),
                Verb.UNUSED_1, 1);

        // make sure we're not tracing when the message is sent (to emulate a receiving host)
        traceCtx().reset();

        final AtomicReference<UUID> reference = new AtomicReference<UUID>();

        // change the local address to emulate another node
        traceCtx().setLocalAddress(InetAddress.getByName("127.0.0.2"));

        MessagingService.instance().registerVerbHandlers(Verb.UNUSED_1, new IVerbHandler<Void>()
        {

            @Override
            public void doVerb(MessageIn<Void> message, String id)
            {
                assertTrue(isTracing());
                assertFalse(traceCtx().isLocalTraceSession());
                assertEquals(sessionId, traceCtx().getSessionId());
                reference.set(traceCtx().trace(
                        new TraceEventBuilder().name("remote trace event").duration(9123L).timestamp(3219L)
                                .build()));
            }
        });

        MessagingService.instance().receive(messageIn, "test_message");
        // message receiving is async so we need to sleep
        Thread.sleep(200);

        assertNotNull(reference.get());

        ColumnFamily family = Table
                .open(TRACE_KEYSPACE)
                .getColumnFamilyStore(EVENTS_TABLE)
                .getColumnFamily(
                        QueryFilter.getIdentityFilter(Util.dk(ByteBuffer.wrap(UUIDGen.decompose(sessionId))),
                                new QueryPath(
                                        EVENTS_TABLE)));

        // Because the MessageDeliveryTask does not run on a DebuggableTPE no automated stage tracing
        // events were inserted, we just have 4 more than after the previous method
        List<TraceEvent> traceEvents = TraceEventBuilder.fromColumnFamily(sessionId, family);
        assertSame(12, traceEvents.size());

        TraceEvent remoteEvent = Iterables.get(traceEvents, 10);
        assertEquals("remote trace event", remoteEvent.name());
        assertEquals(9123L, remoteEvent.duration());
        assertEquals(3219L, remoteEvent.timestamp());
        assertEquals(InetAddress.getByName("127.0.0.2"), remoteEvent.source());
    }

}
