package org.apache.cassandra.service.paxos;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.Table;
import org.apache.cassandra.db.commitlog.ICommitLogEntry;
import org.apache.cassandra.db.commitlog.ReplayPosition;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.UUIDSerializer;

public class PrepareRequest implements ICommitLogEntry
{
    public static PrepareRequestSerializer serializer = new PrepareRequestSerializer();

    public final UUID ballot;
    public final ByteBuffer key;

    public PrepareRequest(UUID ballot, ByteBuffer key)
    {
        this.ballot = ballot;
        this.key = key;
    }

    public long size()
    {
        return serializer.serializedSize(this, MessagingService.current_version);
    }

    public Iterable<ColumnFamily> getColumnFamilies()
    {
        return Collections.emptyList();
    }

    public void write(DataOutputStream out) throws IOException
    {
        serializer.serialize(this, out, MessagingService.current_version);
    }

    public Type getType()
    {
        return Type.paxosPrepare;
    }

    public Runnable getReplayer(long segment, long entryLocation, Map<UUID, ReplayPosition> cfPositions, Set<Table> tablesRecovered, AtomicInteger replayedCount)
    {
        return new Runnable()
        {
            public void run()
            {
                PaxosState state = PaxosState.stateFor(key);
                state.prepare(ballot);
            }
        };
    }

    public static class PrepareRequestSerializer implements IVersionedSerializer<PrepareRequest>
    {
        public void serialize(PrepareRequest request, DataOutput out, int version) throws IOException
        {
            UUIDSerializer.serializer.serialize(request.ballot, out, version);
            ByteBufferUtil.writeWithShortLength(request.key, out);
        }

        public PrepareRequest deserialize(DataInput in, int version) throws IOException
        {
            return new PrepareRequest(UUIDSerializer.serializer.deserialize(in, version),
                                      ByteBufferUtil.readWithShortLength(in));
        }

        public long serializedSize(PrepareRequest request, int version)
        {
            return UUIDSerializer.serializer.serializedSize(request.ballot, version) + 2 + request.key.remaining();
        }
    }
}
