package org.apache.cassandra.db.commitlog;

import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.Table;

public interface ICommitLogEntry
{
    public enum Type
    {
        mutation,
        paxosPrepare,
        paxosPropose,
        paxosCommit;

        private static Type[] types = Type.values();

        public static Type fromOrdinal(int i)
        {
            return types[i];
        }
    }

    public long size();

    public Iterable<ColumnFamily> getColumnFamilies();

    public void write(DataOutputStream out) throws IOException;

    public Type getType();

    public Runnable getReplayer(long segment, long entryLocation, Map<UUID, ReplayPosition> cfPositions, Set<Table> tablesRecovered, AtomicInteger replayedCount);
}
