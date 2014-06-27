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
package org.apache.cassandra.repair;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.repair.messages.SyncComplete;
import org.apache.cassandra.repair.messages.SyncRequest;
import org.apache.cassandra.service.ActiveRepairService;
import org.apache.cassandra.streaming.*;
import org.apache.cassandra.tracing.TraceState;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.utils.FBUtilities;

/**
 * Task that make two nodes exchange (stream) some ranges (for a given table/cf).
 * This handle the case where the local node is neither of the two nodes that
 * must stream their range, and allow to register a callback to be called on
 * completion.
 */
public class StreamingRepairTask implements Runnable, StreamEventHandler
{
    private static final Logger logger = LoggerFactory.getLogger(StreamingRepairTask.class);

    private final TraceState state = Tracing.instance.get();

    /** Repair session ID that this streaming task belongs */
    public final RepairJobDesc desc;
    public final SyncRequest request;

    public StreamingRepairTask(RepairJobDesc desc, SyncRequest request)
    {
        this.desc = desc;
        this.request = request;
    }

    public void run()
    {
        if (request.src.equals(FBUtilities.getBroadcastAddress()))
            initiateStreaming();
        else
            forwardToSource();
    }

    private void initiateStreaming()
    {
        long repairedAt = ActiveRepairService.UNREPAIRED_SSTABLE;
        if (desc.parentSessionId != null && ActiveRepairService.instance.getParentRepairSession(desc.parentSessionId) != null)
            repairedAt = ActiveRepairService.instance.getParentRepairSession(desc.parentSessionId).repairedAt;

        String message;
        logger.info("[streaming task #{}] {}", desc.sessionId, message = String.format("Performing streaming repair of %d ranges with %s", request.ranges.size(), request.dst));
        Tracing.traceRepair(message);
        StreamResultFuture op = new StreamPlan("Repair", repairedAt, 1)
                                    .flushBeforeTransfer(true)
                                    // request ranges from the remote node
                                    .requestRanges(request.dst, desc.keyspace, request.ranges, desc.columnFamily)
                                    // send ranges to the remote node
                                    .transferRanges(request.dst, desc.keyspace, request.ranges, desc.columnFamily)
                                    .execute();
        op.addEventListener(this);
    }

    private void forwardToSource()
    {
        String message;
        logger.info("[repair #{}] {}", desc.sessionId, message = String.format("Forwarding streaming repair of %d ranges to %s (to be streamed with %s)", request.ranges.size(), request.src, request.dst));
        Tracing.traceRepair(message);
        MessagingService.instance().sendOneWay(request.createMessage(), request.src);
    }

    public void handleStreamEvent(StreamEvent event)
    {
        if (state == null)
            return;
        switch (event.eventType)
        {
            case STREAM_PREPARED:
                StreamEvent.SessionPreparedEvent spe = (StreamEvent.SessionPreparedEvent) event;
                state.trace("Streaming session with {} prepared", spe.session.peer);
                break;
            case STREAM_COMPLETE:
                StreamEvent.SessionCompleteEvent sce = (StreamEvent.SessionCompleteEvent) event;
                state.trace("Streaming session with {} {}", sce.peer, sce.success ? "completed successfully" : "failed");
                break;
            case FILE_PROGRESS:
                ProgressInfo pi = ((StreamEvent.ProgressEvent) event).progress;
                state.trace("{}/{} bytes ({}%%) {} idx:{}{}",
                            new Object[] { pi.currentBytes,
                                           pi.totalBytes,
                                           pi.currentBytes * 100 / pi.totalBytes,
                                           pi.direction == ProgressInfo.Direction.OUT ? "sent to" : "received from",
                                           pi.sessionIndex,
                                           pi.peer });
        }
    }

    /**
     * If we succeeded on both stream in and out, reply back to the initiator.
     */
    public void onSuccess(StreamState state)
    {
        String message;
        logger.info("[repair #{}] {}", desc.sessionId, message = String.format("Streaming task succeeded, returning response to %s", request.initiator));
        Tracing.traceRepair(message);
        MessagingService.instance().sendOneWay(new SyncComplete(desc, request.src, request.dst, true).createMessage(), request.initiator);
    }

    /**
     * If we failed on either stream in or out, reply fail to the initiator.
     */
    public void onFailure(Throwable t)
    {
        MessagingService.instance().sendOneWay(new SyncComplete(desc, request.src, request.dst, false).createMessage(), request.initiator);
    }
}
