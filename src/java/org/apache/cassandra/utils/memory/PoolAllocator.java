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

public abstract class PoolAllocator<P extends Pool> extends Allocator
{

    public final P pool;
    public final MemoryOwner onHeap;
    public final MemoryOwner offHeap;
    volatile State state = State.get(LifeCycle.LIVE, Gc.INACTIVE);

    static final AtomicReferenceFieldUpdater<PoolAllocator, State> stateUpdater = AtomicReferenceFieldUpdater.newUpdater(PoolAllocator.class, State.class, "state");

    static enum LifeCycle
    {
        LIVE, DISCARDING, DISCARDED
    }
    static enum Gc
    {
        INACTIVE, MARKING, COLLECTING, FORBIDDEN
    }

    static final class State
    {
        final LifeCycle lifeCycle;
        final Gc gc;

        // Cache not all of the possible combinations of LifeCycle/Gc.
        // Not all of these states are valid, but easier to just create them all.
        private static final State[] ALL;
        private static final int MULT;
        static
        {
            LifeCycle[] lifeCycles = LifeCycle.values();
            Gc[] gcs = Gc.values();
            ALL = new State[lifeCycles.length * gcs.length];
            for (int i = 0 ; i < lifeCycles.length ; i++)
                for (int j = 0 ; j < gcs.length ; j++)
                    ALL[(i * gcs.length) + j] = new State(lifeCycles[i], gcs[j]);
            MULT = gcs.length;
        }

        private State(LifeCycle lifeCycle, Gc gc)
        {
            this.lifeCycle = lifeCycle;
            this.gc = gc;
        }

        private static State get(LifeCycle lifeCycle, Gc gc)
        {
            return ALL[(lifeCycle.ordinal() * MULT) + gc.ordinal()];
        }

        /**
         * maybe transition to the requested Gc state, depending on the current state.
         */
        State transition(Gc targetState)
        {
            switch (targetState)
            {
                case MARKING:
                    // we only permit entering the marking state if GC is not already running for this allocator
                    if (gc != Gc.INACTIVE)
                        return null;
                    // we don't permit GC on an allocator we're discarding, or have discarded
                    if (lifeCycle.compareTo(LifeCycle.DISCARDING) >= 0)
                        return null;
                    return get(lifeCycle, Gc.MARKING);
                case COLLECTING:
                    assert gc == Gc.MARKING;
                    return get(lifeCycle, Gc.COLLECTING);
                case INACTIVE:
                    assert gc == Gc.COLLECTING;
                    return get(lifeCycle, Gc.INACTIVE);
            }
            throw new IllegalStateException();
        }

        State transition(LifeCycle targetState)
        {
            switch (targetState)
            {
                case DISCARDING:
                    assert lifeCycle == LifeCycle.LIVE;
                    return get(LifeCycle.DISCARDING, gc);
                case DISCARDED:
                    assert lifeCycle == LifeCycle.DISCARDING;
                    return get(LifeCycle.DISCARDED, gc);
            }
            throw new IllegalStateException();
        }

        public String toString()
        {
            return lifeCycle + ", GC:" + gc;
        }

    }

    PoolAllocator(P pool)
    {
        this.pool = pool;
        this.onHeap = pool.onHeap.newOwner();
        this.offHeap = pool.offHeap.newOwner();
    }

    /**
     * Mark this allocator reclaiming; this will permit any outstanding allocations to temporarily
     * overshoot the maximum memory limit so that flushing can begin immediately
     */
    public void discarding()
    {
        state = state.transition(LifeCycle.DISCARDING);
        // mark the memory owned by this allocator as reclaiming
        onHeap.markAllReclaiming();
        offHeap.markAllReclaiming();
    }

    public void discarded()
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
        return state.lifeCycle == LifeCycle.LIVE;
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