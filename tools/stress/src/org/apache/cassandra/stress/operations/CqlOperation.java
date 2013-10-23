/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.cassandra.stress.operations;

import java.nio.ByteBuffer;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

import com.google.common.base.*;
import com.google.common.collect.*;
import org.apache.cassandra.stress.Operation;
import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.transport.SimpleClient;
import org.apache.cassandra.transport.messages.ResultMessage;
import org.apache.cassandra.thrift.Compression;
import org.apache.cassandra.thrift.CqlResult;
import org.apache.cassandra.thrift.ThriftConversion;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.Hex;
import org.apache.thrift.TException;

public abstract class CqlOperation extends Operation
{

    private static final ConcurrentHashMap<String, byte[]> preparedStatementLookup = new ConcurrentHashMap<>();

    protected abstract List<String> getQueryParameters(byte[] key);
    protected abstract String buildQuery();
    protected abstract boolean validate(int rowCount);

    public CqlOperation(Settings settings, long idx)
    {
        super(settings, idx);
        if (settings.useSuperColumns)
            throw new IllegalStateException("Super columns are not implemented for CQL");
    }

    private void run(final ClientWrapper wrapper) throws IOException
    {
        if (settings.usePreparedStatements)
        {
            final byte[] id;
            Object idobj = settings.getCqlCache();
            if (idobj == null)
            {
                final String query = buildQuery();
                if (!preparedStatementLookup.containsKey(query))
                {
                    synchronized(preparedStatementLookup)
                    {
                        try
                        {
                            if (!preparedStatementLookup.containsKey(query))
                                preparedStatementLookup.put(query, wrapper.createPreparedStatement(buildQuery()));
                        } catch (TException e)
                        {
                            throw new RuntimeException(e);
                        }
                    }
                }
                id = preparedStatementLookup.get(query);
            }
            else
                id = (byte[]) idobj;

            final byte[] key = getKey().array();
            final List<String> queryParams = getQueryParameters(key);

            timeWithRetry(new RunOp()
            {
                int rowCount;
                @Override
                public boolean run() throws Exception
                {
                    return validate(rowCount = wrapper.execute(id, queryParams));
                }

                @Override
                public String key()
                {
                    return new String(key);
                }

                @Override
                public int keyCount()
                {
                    return rowCount;
                }
            });
        }
        else
        {
            final String query;
            Object qobj = settings.getCqlCache();
            if (qobj == null)
                settings.storeCqlCache(query = buildQuery());
            else
                query = qobj.toString();

            final byte[] key = getKey().array();
            final List<String> queryParams = getQueryParameters(key);

            timeWithRetry(new RunOp()
            {
                int rowCount;
                @Override
                public boolean run() throws Exception
                {
                    return validate(rowCount = wrapper.execute(query, queryParams));
                }

                @Override
                public String key()
                {
                    return new String(key);
                }

                @Override
                public int keyCount()
                {
                    return rowCount;
                }
            });
        }
    }

    public void run(final Cassandra.Client client) throws IOException
    {
        run(settings.isCql3()
                ? new Cql3CassandraClientWrapper(client)
                : new Cql1or2CassandraClientWrapper(client)
        );
    }

    @Override
    public void run(SimpleClient client) throws IOException
    {
        run(new SimpleClientWrapper(client));
    }

    private interface ClientWrapper
    {
        byte[] createPreparedStatement(String cqlQuery) throws TException;
        int execute(byte[] preparedStatementId, List<String> queryParams) throws TException;
        int execute(String query, List<String> queryParams) throws TException;
    }

    private final class SimpleClientWrapper implements ClientWrapper
    {
        final SimpleClient client;
        private SimpleClientWrapper(SimpleClient client)
        {
            this.client = client;
        }

        @Override
        public int execute(String query, List<String> queryParams)
        {
            String formattedQuery = formatCqlQuery(query, queryParams);
            return rowCount(client.execute(formattedQuery, ThriftConversion.fromThrift(settings.consistencyLevel)));
        }

        @Override
        public int execute(byte[] preparedStatementId, List<String> queryParams)
        {
            return rowCount(
                    client.executePrepared(
                            preparedStatementId,
                            queryParamsAsByteBuffer(queryParams),
                            ThriftConversion.fromThrift(settings.consistencyLevel)));
        }

        @Override
        public byte[] createPreparedStatement(String cqlQuery)
        {
            return client.prepare(cqlQuery).statementId.bytes;
        }
    }

    private final class Cql3CassandraClientWrapper implements ClientWrapper
    {
        final Cassandra.Client client;
        private Cql3CassandraClientWrapper(Cassandra.Client client)
        {
            this.client = client;
        }

        @Override
        public int execute(String query, List<String> queryParams) throws TException
        {
            String formattedQuery = formatCqlQuery(query, queryParams);
            return rowCount(
                client.execute_cql3_query(ByteBuffer.wrap(formattedQuery.getBytes()), Compression.NONE, settings.consistencyLevel)
            );
        }

        @Override
        public int execute(byte[] preparedStatementId, List<String> queryParams) throws TException
        {
            Integer id = fromBytes(preparedStatementId);
            return rowCount(
                    client.execute_prepared_cql3_query(id, queryParamsAsByteBuffer(queryParams), settings.consistencyLevel)
            );
        }

        @Override
        public byte[] createPreparedStatement(String cqlQuery) throws TException
        {
            return toBytes(client.prepare_cql3_query(ByteBufferUtil.bytes(cqlQuery), Compression.NONE).itemId);
        }
    }

    private final class Cql1or2CassandraClientWrapper implements ClientWrapper
    {
        final Cassandra.Client client;
        private Cql1or2CassandraClientWrapper(Cassandra.Client client)
        {
            this.client = client;
        }

        @Override
        public int execute(String query, List<String> queryParams) throws TException
        {
            String formattedQuery = formatCqlQuery(query, queryParams);
            return rowCount(
                    client.execute_cql_query(ByteBuffer.wrap(formattedQuery.getBytes()), Compression.NONE)
            );
        }

        @Override
        public int execute(byte[] preparedStatementId, List<String> queryParams) throws TException
        {
            Integer id = fromBytes(preparedStatementId);
            return rowCount(
                    client.execute_prepared_cql_query(id, queryParamsAsByteBuffer(queryParams))
            );
        }

        @Override
        public byte[] createPreparedStatement(String cqlQuery) throws TException
        {
            return toBytes(client.prepare_cql_query(ByteBufferUtil.bytes(cqlQuery), Compression.NONE).itemId);
        }
    }

    private static int rowCount(ResultMessage result)
    {
        return result instanceof ResultMessage.Rows ? ((ResultMessage.Rows) result).result.size() : 0;
    }

    private static int rowCount(CqlResult result)
    {
        return result.getRows().size();
    }

    private static Integer fromBytes(byte[] bytes)
    {
        return bytes[0] | (bytes[1] << 8)
             | (bytes[2] << 16) | (bytes[3] << 24);
    }

    private static byte[] toBytes(int integer)
    {
        return new byte[] {
                (byte)(integer & 0xFF),
                (byte)((integer >> 8) & 0xFF),
                (byte) ((integer >> 16) & 0xFF),
                (byte) ((integer >> 24) & 0xFF)
        };
    }

    protected String getUnQuotedCqlBlob(String term, boolean isCQL3)
    {
        return getUnQuotedCqlBlob(term.getBytes(), isCQL3);
    }

    protected String getUnQuotedCqlBlob(byte[] term, boolean isCQL3)
    {
        return isCQL3
                ? "0x" + Hex.bytesToHex(term)
                : Hex.bytesToHex(term);
    }

    protected List<ByteBuffer> queryParamsAsByteBuffer(List<String> queryParams)
    {
        return Lists.transform(queryParams, new Function<String, ByteBuffer>()
        {
            public ByteBuffer apply(String param)
            {
                if (param.startsWith("0x"))
                    param = param.substring(2);
                return ByteBufferUtil.hexToBytes(param);
            }
        });
    }

    /**
     * Constructs a CQL query string by replacing instances of the character
     * '?', with the corresponding parameter.
     *
     * @param query base query string to format
     * @param parms sequence of string query parameters
     * @return formatted CQL query string
     */
    protected static String formatCqlQuery(String query, List<String> parms)
    {
        int marker, position = 0;
        StringBuilder result = new StringBuilder();

        if (-1 == (marker = query.indexOf('?')) || parms.size() == 0)
            return query;

        for (String parm : parms)
        {
            result.append(query.substring(position, marker));
            result.append(parm);

            position = marker + 1;
            if (-1 == (marker = query.indexOf('?', position + 1)))
                break;
        }

        if (position < query.length())
            result.append(query.substring(position));

        return result.toString();
    }

    protected String wrapInQuotesIfRequired(String string)
    {
        return settings.cqlVersion == CqlVersion.CQL3
                ? "\"" + string + "\""
                : string;
    }

}
