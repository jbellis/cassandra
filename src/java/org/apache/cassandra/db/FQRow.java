package org.apache.cassandra.db;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.utils.ByteBufferUtil;

/**
 * A fully-qualified row is a row including its table (keyspace)
 */
public class FQRow
{
    public static final FQRowSerializer serializer = new FQRowSerializer();

    public final String table;
    public final ByteBuffer key;
    public final ColumnFamily columns;

    public FQRow(String table, ByteBuffer key, ColumnFamily columns)
    {
        this.table = table;
        this.key = key;
        this.columns = columns;
    }

    public static class FQRowSerializer implements IVersionedSerializer<FQRow>
    {
        public void serialize(FQRow row, DataOutput out, int version) throws IOException
        {
            if (row == null)
            {
                out.writeByte(0);
                return;
            }

            out.writeByte(1);
            out.writeUTF(row.table);
            ByteBufferUtil.writeWithShortLength(row.key, out);
            ColumnFamily.serializer.serialize(row.columns, out, version);
        }

        public FQRow deserialize(DataInput in, int version) throws IOException
        {
            boolean isNull = (in.readByte() == 0);
            if (isNull)
                return null;
            return new FQRow(in.readUTF(),
                             ByteBufferUtil.readWithShortLength(in),
                             ColumnFamily.serializer.deserialize(in, version));
        }

        public long serializedSize(FQRow row, int version)
        {
            if (row == null)
                return 1;

            return 1
                   + TypeSizes.encodedUTF8Length(row.table)
                   + 2 + row.key.remaining()
                   + ColumnFamily.serializer.serializedSize(row.columns, version);
        }
    }
}
