package org.apache.cassandra.db.compaction;

import java.io.IOException;
import java.net.InetAddress;
import java.util.Collections;
import java.util.List;

import org.junit.BeforeClass;
import org.junit.Test;

import junit.framework.Assert;
import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.Util;
import org.apache.cassandra.db.*;
import org.apache.cassandra.dht.BytesToken;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.locator.TokenMetadata;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.ByteBufferUtil;

import static junit.framework.Assert.assertEquals;

public class CompactionsRangePurgeTest
{
    public static final String TABLE1 = "Keyspace1";

    @BeforeClass
    public static void init() throws IOException
    {
        CompactionManager.init(new IRangeProvider()
        {
            public Iterable<Range<Token>> getRanges(Table table)
            {
                return Collections.singleton(new Range<Token>(new BytesToken(new byte[] {1}), new BytesToken(new byte[] {2})));
            }
        });

        SchemaLoader.loadSchema(false);
    }

    @Test
    public void testCleanupDuringCompaction() throws Exception
    {
        CompactionManager.instance().disableAutoCompaction();
        Table table = Table.open(TABLE1);
        String cfName = "Standard1";
        ColumnFamilyStore cfs = table.getColumnFamilyStore(cfName);
        // inserts
        for (int i = 0; i < 10; i++)
        {
            DecoratedKey key = Util.dk("key" + i);
            RowMutation rm = new RowMutation(TABLE1, key.key);
            rm.add(cfName, ByteBufferUtil.bytes(String.valueOf(i)), ByteBufferUtil.EMPTY_BYTE_BUFFER, 0);
            rm.apply();
        }
        cfs.forceBlockingFlush();

        List<Row> rows = Util.getRangeSlice(cfs);
        assertEquals(10, rows.size());
        CompactionManager.instance().submitMaximal(cfs, Integer.MAX_VALUE).get();
        rows = Util.getRangeSlice(cfs);
        assertEquals(0, rows.size());
    }
}
