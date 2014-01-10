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
package org.apache.cassandra.utils.memory;

import org.apache.cassandra.utils.concurrent.OpOrdering;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.utils.ByteBufferUtil;

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

public abstract class PoolAllocator<P extends Pool> extends AbstractAllocator
{
    public final P pool;
    public final MemoryOwner onHeap;
    public final MemoryOwner offHeap;
    volatile LifeCycle state = LifeCycle.LIVE;

    static enum LifeCycle
    {
        LIVE, DISCARDING, DISCARDED;
        LifeCycle transition(LifeCycle target)
        {
            assert target.ordinal() == ordinal() + 1;
            return target;
        }
    }

    PoolAllocator(P pool)
    {
        this.pool = pool;
        this.onHeap = pool.onHeap.newOwner();
        this.offHeap = pool.offHeap.newOwner();
    }

    /**
     * Mark this allocator as reclaiming; this will mark the memory it owns as reclaiming, so remove it from
     * any calculation deciding if further cleaning/reclamation is necessary.
     */
    public void setDiscarding()
    {
        state = state.transition(LifeCycle.DISCARDING);
        // mark the memory owned by this allocator as reclaiming
        onHeap.markAllReclaiming();
        offHeap.markAllReclaiming();
    }

    /**
     * Indicate the memory and resources owned by this allocator are no longer referenced,
     * and can be reclaimed/reused.
     */
    public void setDiscarded()
    {
        state = state.transition(LifeCycle.DISCARDED);
        // release any memory owned by this allocator; automatically signals waiters
        onHeap.releaseAll();
        offHeap.releaseAll();
    }

    public abstract ByteBuffer allocate(int size, OpOrdering.Ordered writeOp);

    /** Mark the BB as unused, permitting it to be reclaimed */
    public abstract void free(ByteBuffer name);

    public boolean isLive()
    {
        return state == LifeCycle.LIVE;
    }

    /**
     * Allocate a slice of the given length.
     */
    public ByteBuffer clone(ByteBuffer buffer, OpOrdering.Ordered writeOp)
    {
        assert buffer != null;
        if (buffer.remaining() == 0)
            return ByteBufferUtil.EMPTY_BYTE_BUFFER;
        ByteBuffer cloned = allocate(buffer.remaining(), writeOp);

        cloned.mark();
        cloned.put(buffer.duplicate());
        cloned.reset();
        return cloned;
    }

    public ContextAllocator wrap(OpOrdering.Ordered writeOp, ColumnFamilyStore cfs)
    {
        return new ContextAllocator(writeOp, this, cfs);
    }
}