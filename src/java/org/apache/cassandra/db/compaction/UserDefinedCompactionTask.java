package org.apache.cassandra.db.compaction;

import java.util.Collection;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.io.sstable.SSTableReader;

class UserDefinedCompactionTask extends CompactionTask
{
    public UserDefinedCompactionTask(ColumnFamilyStore cfs, Collection<SSTableReader> sstables, int gcBefore)
    {
        super(cfs, sstables, gcBefore);
    }

    protected boolean partialCompactionsAcceptable()
    {
        return false;
    }
}
