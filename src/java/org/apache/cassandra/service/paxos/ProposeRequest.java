package org.apache.cassandra.service.paxos;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.FQRow;
import org.apache.cassandra.db.Table;
import org.apache.cassandra.db.commitlog.CommitLog;
import org.apache.cassandra.db.commitlog.ICommitLogEntry;
import org.apache.cassandra.db.commitlog.ReplayPosition;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.utils.UUIDSerializer;

public class ProposeRequest implements ICommitLogEntry
{
    public static final ProposeRequestSerializer serializer = new ProposeRequestSerializer();

    public final UUID ballot;
    public final FQRow proposal;

    public ProposeRequest(UUID ballot, FQRow proposal)
    {
        this.ballot = ballot;
        this.proposal = proposal;
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
        return Type.paxosPropose;
    }

    public Runnable getReplayer(long segment, long entryLocation, Map<UUID, ReplayPosition> cfPositions, Set<Table> tablesRecovered, AtomicInteger replayedCount)
    {
        return new Runnable()
        {
            public void run()
            {
                PaxosState state = PaxosState.stateFor(proposal.key);
                state.propose(ballot, proposal);
            }
        };
    }

    public static class ProposeRequestSerializer implements IVersionedSerializer<ProposeRequest>
    {
        public void serialize(ProposeRequest proposal, DataOutput out, int version) throws IOException
        {
            UUIDSerializer.serializer.serialize(proposal.ballot, out, version);
            FQRow.serializer.serialize(proposal.proposal, out, version);
        }

        public ProposeRequest deserialize(DataInput in, int version) throws IOException
        {
            return new ProposeRequest(UUIDSerializer.serializer.deserialize(in, version),
                                     FQRow.serializer.deserialize(in, version));
        }

        public long serializedSize(ProposeRequest proposal, int version)
        {
            return UUIDSerializer.serializer.serializedSize(proposal.ballot, version)
                   + FQRow.serializer.serializedSize(proposal.proposal, version);
        }
    }
}
