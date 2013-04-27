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
package org.apache.cassandra.io.sstable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.utils.ByteBufferUtil;

public class IndexSummary
{
    public static final IndexSummarySerializer serializer = new IndexSummarySerializer();

    private final long[] positions;
    private final List<DecoratedKey> keys;

    public IndexSummary(List<DecoratedKey> keys, long[] positions)
    {
        this.keys = keys;
        this.positions = positions;
    }

    public List<DecoratedKey> getKeys()
    {
        return keys;
    }

    public long getPosition(int index)
    {
        return positions[index];
    }

    public static class IndexSummarySerializer
    {
        public void serialize(IndexSummary t, DataOutput dos) throws IOException
        {
            assert t.keys.size() == t.keys.size() : "keysize and the position sizes are not same.";
            dos.writeInt(DatabaseDescriptor.getIndexInterval());
            dos.writeInt(t.keys.size());
            for (int i = 0; i < t.keys.size(); i++)
            {
                dos.writeLong(t.getPosition(i));
                ByteBufferUtil.writeWithLength(t.keys.get(i).key, dos);
            }
        }

        public IndexSummary deserialize(DataInput dis, IPartitioner partitioner) throws IOException
        {
            if (dis.readInt() != DatabaseDescriptor.getIndexInterval())
                throw new IOException("Cannot read the saved summary because Index Interval changed.");

            int size = dis.readInt();
            long[] positions = new long[size];
            List<DecoratedKey> keys = new ArrayList<DecoratedKey>(size);

            for (int i = 0; i < size; i++)
            {
                positions[i] = dis.readLong();
                keys.add(partitioner.decorateKey(ByteBufferUtil.readWithLength(dis)));
            }

            return new IndexSummary(keys, positions);
        }
    }
}
