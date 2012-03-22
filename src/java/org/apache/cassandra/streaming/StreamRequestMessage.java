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
package org.apache.cassandra.streaming;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import com.google.common.collect.Iterables;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Table;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.net.CompactEndpointSerializationHelper;
import org.apache.cassandra.net.MessageOut;
import org.apache.cassandra.net.MessagingService;

/**
* This class encapsulates the message that needs to be sent to nodes
* that handoff data. The message contains information about ranges
* that need to be transferred and the target node.
*
* If a file is specified, ranges and table will not. vice-versa should hold as well.
*/
public class StreamRequestMessage // TODO rename to StreamRequest
{
    private static final IVersionedSerializer<StreamRequestMessage> serializer;
    static
    {
        serializer = new StreamRequestMessageSerializer();
    }

    public static IVersionedSerializer<StreamRequestMessage> serializer()
    {
        return serializer;
    }

    protected final long sessionId;
    protected final InetAddress target;

    // if this is specified, ranges and table should not be.
    protected final PendingFile file;

    // if these are specified, file shoud not be.
    protected final Collection<Range<Token>> ranges;
    protected final String table;
    protected final Iterable<ColumnFamilyStore> columnFamilies;
    protected final OperationType type;

    StreamRequestMessage(InetAddress target, Collection<Range<Token>> ranges, String table, Iterable<ColumnFamilyStore> columnFamilies, long sessionId, OperationType type)
    {
        this.target = target;
        this.ranges = ranges;
        this.table = table;
        this.columnFamilies = columnFamilies;
        this.sessionId = sessionId;
        this.type = type;
        file = null;
    }

    StreamRequestMessage(InetAddress target, PendingFile file, long sessionId)
    {
        this.target = target;
        this.file = file;
        this.sessionId = sessionId;
        this.type = file.type;
        ranges = null;
        table = null;
        columnFamilies = null;
    }

    public MessageOut<StreamRequestMessage> createMessage()
    {
        return new MessageOut<StreamRequestMessage>(MessagingService.Verb.STREAM_REQUEST, this, serializer);
    }

    public String toString()
    {
        StringBuilder sb = new StringBuilder("");
        if (file == null)
        {
            sb.append(table);
            sb.append("@");
            sb.append(columnFamilies.toString());
            sb.append("@");
            sb.append(target);
            sb.append("------->");
            for ( Range<Token> range : ranges )
            {
                sb.append(range);
                sb.append(" ");
            }
            sb.append(type);
        }
        else
        {
            sb.append(file.toString());
        }
        return sb.toString();
    }

    private static class StreamRequestMessageSerializer implements IVersionedSerializer<StreamRequestMessage>
    {
        public void serialize(StreamRequestMessage srm, DataOutput dos, int version) throws IOException
        {
            dos.writeLong(srm.sessionId);
            CompactEndpointSerializationHelper.serialize(srm.target, dos);
            if (srm.file != null)
            {
                dos.writeBoolean(true);
                PendingFile.serializer().serialize(srm.file, dos, version);
            }
            else
            {
                dos.writeBoolean(false);
                dos.writeUTF(srm.table);
                dos.writeInt(srm.ranges.size());
                for (Range<Token> range : srm.ranges)
                {
                    AbstractBounds.serializer().serialize(range, dos, version);
                }

                if (version > MessagingService.VERSION_07)
                    dos.writeUTF(srm.type.name());

                if (version > MessagingService.VERSION_080)
                {
                    dos.writeInt(Iterables.size(srm.columnFamilies));
                    for (ColumnFamilyStore cfs : srm.columnFamilies)
                        dos.writeInt(cfs.metadata.cfId);
                }
            }
        }

        public StreamRequestMessage deserialize(DataInput dis, int version) throws IOException
        {
            long sessionId = dis.readLong();
            InetAddress target = CompactEndpointSerializationHelper.deserialize(dis);
            boolean singleFile = dis.readBoolean();
            if (singleFile)
            {
                PendingFile file = PendingFile.serializer().deserialize(dis, version);
                return new StreamRequestMessage(target, file, sessionId);
            }
            else
            {
                String table = dis.readUTF();
                int size = dis.readInt();
                List<Range<Token>> ranges = (size == 0) ? null : new ArrayList<Range<Token>>(size);
                for( int i = 0; i < size; ++i )
                {
                    ranges.add((Range<Token>) AbstractBounds.serializer().deserialize(dis, version).toTokenBounds());
                }
                OperationType type = OperationType.RESTORE_REPLICA_COUNT;
                if (version > MessagingService.VERSION_07)
                    type = OperationType.valueOf(dis.readUTF());

                List<ColumnFamilyStore> stores = new ArrayList<ColumnFamilyStore>();
                if (version > MessagingService.VERSION_080)
                {
                    int cfsSize = dis.readInt();
                    for (int i = 0; i < cfsSize; ++i)
                        stores.add(Table.open(table).getColumnFamilyStore(dis.readInt()));
                }

                return new StreamRequestMessage(target, ranges, table, stores, sessionId, type);
            }
        }

        public long serializedSize(StreamRequestMessage streamRequestMessage, int version)
        {
            throw new UnsupportedOperationException();
        }
    }
}
