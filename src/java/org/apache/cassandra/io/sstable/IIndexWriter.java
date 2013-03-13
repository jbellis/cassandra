package org.apache.cassandra.io.sstable;

import java.io.Closeable;

import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.RowIndexEntry;

public interface IIndexWriter extends Closeable
{
    void append(DecoratedKey key, RowIndexEntry indexEntry);

    void mark();

    void resetAndTruncate();
}
