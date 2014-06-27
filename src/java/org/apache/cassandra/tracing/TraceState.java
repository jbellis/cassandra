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

import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Stopwatch;
import org.slf4j.helpers.MessageFormatter;

import org.apache.cassandra.concurrent.Stage;
import org.apache.cassandra.concurrent.StageManager;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.db.ArrayBackedSortedColumns;
import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.UUIDGen;
import org.apache.cassandra.utils.WrappedRunnable;

/**
 * ThreadLocal state for a tracing session. The presence of an instance of this class as a ThreadLocal denotes that an
 * operation is being traced.
 */
public class TraceState
{
    public final UUID sessionId;
    public final InetAddress coordinator;
    public final Stopwatch watch;
    public final ByteBuffer sessionIdBytes;
    public final Tracing.TraceType traceType;
    public final int ttl;

    private boolean notify;
    private Object notificationHandle;

    private boolean done;
    private boolean hasNotifications;
    private long wait = minWaitMillis;

    private static long minWaitMillis = 16 * 1024L;
    private static long maxWaitMillis = 1000 * 1024L;

    public TraceState(InetAddress coordinator, UUID sessionId)
    {
        this(coordinator, sessionId, Tracing.TraceType.QUERY);
    }

    public TraceState(InetAddress coordinator, UUID sessionId, Tracing.TraceType traceType)
    {
        this(coordinator, sessionId, traceType, traceType.getTTL());
    }

    public TraceState(InetAddress coordinator, UUID sessionId, Tracing.TraceType traceType, int ttl)
    {
        assert coordinator != null;
        assert sessionId != null;

        this.coordinator = coordinator;
        this.sessionId = sessionId;
        sessionIdBytes = ByteBufferUtil.bytes(sessionId);
        this.traceType = traceType;
        this.ttl = ttl;
        watch = Stopwatch.createStarted();
    }

    public void enableNotifications()
    {
        assert traceType == Tracing.TraceType.REPAIR;
        notify = true;
    }

    public void setNotificationHandle(Object handle)
    {
        assert traceType == Tracing.TraceType.REPAIR;
        notificationHandle = handle;
    }

    public int elapsed()
    {
        long elapsed = watch.elapsed(TimeUnit.MICROSECONDS);
        return elapsed < Integer.MAX_VALUE ? (int) elapsed : Integer.MAX_VALUE;
    }

    public synchronized void stop()
    {
        done = true;
        notifyAll();
    }

    public synchronized boolean isDone()
    {
        boolean haveWaited = false;
        while (true)
        {
            if (hasNotifications)
            {
                hasNotifications = false;
                wait = minWaitMillis;
                return false;
            }
            else if (done)
            {
                return true;
            }
            else if (haveWaited)
            {
                wait = Math.min((wait ^ 1) << (wait & 1), maxWaitMillis);
                return false;
            }
            else
            {
                haveWaited = true;
                try
                {
                    wait(wait & ~1);
                }
                catch (InterruptedException e)
                {
                    throw new RuntimeException();
                }
            }
        }
    }

    private synchronized void notifyActivity()
    {
        hasNotifications = true;
        notifyAll();
    }

    public void trace(String format, Object arg)
    {
        trace(MessageFormatter.format(format, arg).getMessage());
    }

    public void trace(String format, Object arg1, Object arg2)
    {
        trace(MessageFormatter.format(format, arg1, arg2).getMessage());
    }

    public void trace(String format, Object[] args)
    {
        trace(MessageFormatter.arrayFormat(format, args).getMessage());
    }

    public void trace(String message)
    {
        if (notify)
            notifyActivity();

        TraceState.trace(sessionIdBytes, message, elapsed(), ttl, notificationHandle);
    }

    public static void trace(final ByteBuffer sessionIdBytes, final String message, final int elapsed, final int ttl, final Object notificationHandle)
    {
        final ByteBuffer eventId = ByteBuffer.wrap(UUIDGen.getTimeUUIDBytes());
        final String threadName = Thread.currentThread().getName();

        if (notificationHandle != null)
            StorageService.instance.sendNotification("repair", message, notificationHandle);

        StageManager.getStage(Stage.TRACING).execute(new WrappedRunnable()
        {
            public void runMayThrow()
            {
                CFMetaData cfMeta = CFMetaData.TraceEventsCf;
                ColumnFamily cf = ArrayBackedSortedColumns.factory.create(cfMeta);
                Tracing.addColumn(cf, Tracing.buildName(cfMeta, eventId, ByteBufferUtil.bytes("activity")), message, ttl);
                Tracing.addColumn(cf, Tracing.buildName(cfMeta, eventId, ByteBufferUtil.bytes("source")), FBUtilities.getBroadcastAddress(), ttl);
                if (elapsed >= 0)
                    Tracing.addColumn(cf, Tracing.buildName(cfMeta, eventId, ByteBufferUtil.bytes("source_elapsed")), elapsed, ttl);
                Tracing.addColumn(cf, Tracing.buildName(cfMeta, eventId, ByteBufferUtil.bytes("thread")), threadName, ttl);
                Tracing.mutateWithCatch(new Mutation(Tracing.TRACE_KS, sessionIdBytes, cf));
            }
        });
    }
}
