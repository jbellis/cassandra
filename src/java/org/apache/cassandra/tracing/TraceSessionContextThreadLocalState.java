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

import java.net.InetAddress;
import java.util.UUID;

import com.google.common.base.Stopwatch;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * ThreadLocal state for a tracing session. The presence of an instance of this class as a ThreadLocal denotes that an
 * operation is being traced.
 */
public class TraceSessionContextThreadLocalState
{
    public static final Logger logger = LoggerFactory.getLogger(TraceSessionContextThreadLocalState.class);

    public final UUID sessionId;
    public final InetAddress origin;
    public final InetAddress source;
    public final String messageId;
    public final Stopwatch watch;

    public TraceSessionContextThreadLocalState(final TraceSessionContextThreadLocalState other)
    {
        this(other.origin, other.source, other.sessionId, other.messageId);
    }

    public TraceSessionContextThreadLocalState(final InetAddress coordinator, final InetAddress source,
            final UUID sessionId)
    {
        this(coordinator, source, sessionId, null);
    }

    public TraceSessionContextThreadLocalState(final InetAddress coordinator, final InetAddress source,
            final UUID sessionId,
            final String messageId)
    {
        checkNotNull(coordinator);
        checkNotNull(source);
        checkNotNull(sessionId);
        this.origin = coordinator;
        this.source = source;
        this.sessionId = sessionId;
        this.messageId = ((messageId == null) || (messageId.length() == 0)) ? null : messageId;
        this.watch = new Stopwatch();
        this.watch.start();
    }
}
