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
package org.apache.cassandra.hadoop.cql3;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.util.*;

import com.google.common.collect.AbstractIterator;
import com.google.common.collect.Iterables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.CompositeType;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.db.marshal.TypeParser;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.exceptions.SyntaxException;
import org.apache.cassandra.hadoop.ColumnFamilySplit;
import org.apache.cassandra.hadoop.ConfigHelper;
import org.apache.cassandra.thrift.*;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.thrift.TException;
import org.apache.thrift.transport.TTransport;

/**
 * Hadoop RecordReader read the values return from the CQL query
 * It use CQL key range query to page through the wide rows.
 * <p/>
 * Return List<IColumn> as keys columns
 * <p/>
 * Map<ByteBuffer, IColumn> as column name to columns mappings
 */
public class ColumnFamilyRecordReader extends RecordReader<Map<String, ByteBuffer>, Map<String, ByteBuffer>>
        implements org.apache.hadoop.mapred.RecordReader<Map<String, ByteBuffer>, Map<String, ByteBuffer>>
{
    private static final Logger logger = LoggerFactory.getLogger(ColumnFamilyRecordReader.class);

    public static final int DEFAULT_CQL_PAGE_LIMIT = 1000; // TODO: find the number large enough but not OOM

    private ColumnFamilySplit split;
    private RowIterator rowIterator;

    private Pair<Map<String, ByteBuffer>, Map<String, ByteBuffer>> currentRow;
    private int totalRowCount; // total number of rows to fetch
    private String keyspace;
    private String cfName;
    private Cassandra.Client client;
    private ConsistencyLevel consistencyLevel;

    // partition keys -- key aliases
    private List<Key> partitionKeys = new ArrayList<Key>();

    // cluster keys -- column aliases
    private List<Key> clusterColumns = new ArrayList<Key>();

    // map prepared query type to item id
    private Map<Integer, Integer> preparedQueryIds = new HashMap<Integer, Integer>();

    // cql query select columns
    private String columns;

    // the number of cql rows per page
    private int pageRowSize;

    // user defined where clauses
    private String userDefinedWhereClauses;

    private IPartitioner partitioner;

    private AbstractType<?> keyValidator;

    public ColumnFamilyRecordReader()
    {
        super();
    }

    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException
    {
        this.split = (ColumnFamilySplit) split;
        Configuration conf = context.getConfiguration();
        totalRowCount = (this.split.getLength() < Long.MAX_VALUE)
                      ? (int) this.split.getLength()
                      : ConfigHelper.getInputSplitSize(conf);
        cfName = ConfigHelper.getInputColumnFamily(conf);
        consistencyLevel = ConsistencyLevel.valueOf(ConfigHelper.getReadConsistencyLevel(conf));
        keyspace = ConfigHelper.getInputKeyspace(conf);
        columns = CQLConfigHelper.getInputcolumns(conf);
        userDefinedWhereClauses = CQLConfigHelper.getInputWhereClauses(conf);

        try
        {
            pageRowSize = Integer.parseInt(CQLConfigHelper.getInputPageRowSize(conf));
        }
        catch (NumberFormatException e)
        {
            pageRowSize = DEFAULT_CQL_PAGE_LIMIT;
        }

        partitioner = ConfigHelper.getInputPartitioner(context.getConfiguration());

        try
        {
            if (client != null)
                return;

            // create connection using thrift
            String location = getLocation();

            int port = ConfigHelper.getInputRpcPort(conf);
            client = ColumnFamilyInputFormat.createAuthenticatedClient(location, port, conf);

            // retrieve partition keys and cluster keys from system.schema_columnfamilies table
            retrieveKeys();

            client.set_keyspace(keyspace);
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }

        rowIterator = new RowIterator();

        logger.debug("created {}", rowIterator);
    }

    public void close()
    {
        if (client != null)
        {
            TTransport transport = client.getOutputProtocol().getTransport();
            if (transport.isOpen())
                transport.close();
            client = null;
        }
    }

    public Map<String, ByteBuffer> getCurrentKey()
    {
        return currentRow.left;
    }

    public Map<String, ByteBuffer> getCurrentValue()
    {
        return currentRow.right;
    }

    public float getProgress()
    {
        if (!rowIterator.hasNext())
            return 1.0F;

        // the progress is likely to be reported slightly off the actual but close enough
        float progress = ((float) rowIterator.totalRead / totalRowCount);
        return progress > 1.0F ? 1.0F : progress;
    }

    public boolean nextKeyValue() throws IOException
    {
        if (!rowIterator.hasNext())
        {
            logger.debug("Finished scanning " + rowIterator.totalRead + " rows (estimate was: " + totalRowCount + ")");
            return false;
        }

        currentRow = rowIterator.next();
        return true;
    }

    // we don't use endpointsnitch since we are trying to support hadoop nodes that are
    // not necessarily on Cassandra machines, too.  This should be adequate for single-DC clusters, at least.
    private String getLocation()
    {
        Collection<InetAddress> localAddresses = FBUtilities.getAllLocalAddresses();

        for (InetAddress address : localAddresses)
        {
            for (String location : split.getLocations())
            {
                InetAddress locationAddress;
                try
                {
                    locationAddress = InetAddress.getByName(location);
                }
                catch (UnknownHostException e)
                {
                    throw new AssertionError(e);
                }
                if (address.equals(locationAddress))
                {
                    return location;
                }
            }
        }
        return split.getLocations()[0];
    }

    // Because the old Hadoop API wants us to write to the key and value
    // and the new asks for them, we need to copy the output of the new API
    // to the old. Thus, expect a small performance hit.
    // And obviously this wouldn't work for wide rows. But since ColumnFamilyInputFormat
    // and ColumnFamilyRecordReader don't support them, it should be fine for now.
    public boolean next(Map<String, ByteBuffer> keys, Map<String, ByteBuffer> value) throws IOException
    {
        if (nextKeyValue())
        {
            value.clear();
            value.putAll(getCurrentValue());
            
            keys.clear();
            keys.putAll(getCurrentKey());

            return true;
        }
        return false;
    }

    public long getPos() throws IOException
    {
        return (long) rowIterator.totalRead;
    }

    public Map<String, ByteBuffer> createKey()
    {
        return new LinkedHashMap<String, ByteBuffer>();
    }

    public Map<String, ByteBuffer> createValue()
    {
        return new LinkedHashMap<String, ByteBuffer>();
    }

    /** CQL row iterator */
    private class RowIterator extends AbstractIterator<Pair<Map<String, ByteBuffer>, Map<String, ByteBuffer>>>
    {
        protected int totalRead = 0;             // total number of cf rows read
        protected Iterator<CqlRow> rows;
        private int pageRows = 0;                // the number of cql rows read of this page
        private String previousRowKey = null;    // previous CF row key
        private String partitionKeyString;       // keys in <key1>, <key2>, <key3> string format
        private String partitionKeyMarkers;    // question marks in ? , ? , ? format which matches the number of keys

        public RowIterator()
        {
            // initial page
            rows = executeQuery();
        }

        protected Pair<Map<String, ByteBuffer>, Map<String, ByteBuffer>> computeNext()
        {
            if (rows == null)
                return endOfData();

            int index = -2;
            //check there are more page to read
            while (!rows.hasNext())
            {
                // no more data
                if (index == -1 || emptyPartitionKeyValues())
                {
                    logger.debug("no more data.");
                    return endOfData();
                }

                index = setTailNull(clusterColumns);
                logger.debug("set tail to null, index: " + index);
                rows = executeQuery();
                pageRows = 0;

                if (rows == null || !rows.hasNext() && index < 0)
                {
                    logger.debug("no more data.");
                    return endOfData();
                }
            }

            Map<String, ByteBuffer> valueColumns = createValue();
            Map<String, ByteBuffer> keyColumns = createKey();
            int i = 0;
            CqlRow row = rows.next();
            for (Column column : row.columns)
            {
                String columnName = stringValue(ByteBuffer.wrap(column.getName()));
                logger.debug("column: " + columnName);

                if (i < partitionKeys.size() + clusterColumns.size())
                    keyColumns.put(stringValue(column.name), column.value);
                else
                    valueColumns.put(stringValue(column.name), column.value);

                i++;
            }

            // increase total CQL row read for this page
            pageRows++;

            // increase total CF row read
            if (newRow(keyColumns, previousRowKey))
                totalRead++;

            // read full page
            if (pageRows >= pageRowSize || !rows.hasNext())
            {
                // update partition keys
                Iterator<String> newKeys = keyColumns.keySet().iterator();
                Iterator<Key> keys = partitionKeys.iterator();
                while (keys.hasNext())
                {
                    keys.next().value = keyColumns.get(newKeys.next());
                }

                // update cluster keys
                keys = clusterColumns.iterator();
                while (keys.hasNext())
                {
                    Key key = keys.next();
                    key.value = keyColumns.get(newKeys.next());
                }

                rows = executeQuery();
                pageRows = 0;
            }

            return Pair.create(keyColumns, valueColumns);
        }

        /** check whether start to read a new CF row by comparing the partition keys */
        private boolean newRow(Map<String, ByteBuffer> keyColumns, String previousRowKey)
        {
            if (keyColumns.isEmpty())
                return false;

            String rowKey = "";
            if (keyColumns.size() == 1)
            {
                rowKey = partitionKeys.get(0).validator.getString(keyColumns.get(partitionKeys.get(0).name));
            }
            else
            {
                Iterator<ByteBuffer> iter = keyColumns.values().iterator();
                for (Key key : partitionKeys)
                    rowKey = rowKey + key.validator.getString(ByteBufferUtil.clone(iter.next())) + ":";
            }

            logger.debug("previous RowKey: " + previousRowKey + ", new row key: " + rowKey);
            if (previousRowKey == null)
            {
                this.previousRowKey = rowKey;
                return true;
            }

            if (rowKey.equals(previousRowKey))
                return false;

            this.previousRowKey = rowKey;
            return true;
        }

        /** set the last non-null key value to null, and return the previous index */
        private int setTailNull(List<Key> values)
        {
            if (values.isEmpty())
                return -1;

            Iterator<Key> iterator = values.iterator();
            int previousIndex = -1;
            Key current;
            while (iterator.hasNext())
            {
                current = iterator.next();
                if (current.value == null)
                {
                    int index = previousIndex > 0 ? previousIndex : 0;
                    Key key = values.get(index);
                    logger.debug("set key " + key.name + " value to  null");
                    key.value = null;
                    return previousIndex - 1;
                }

                previousIndex++;
            }

            Key key = values.get(previousIndex);
            logger.debug("set key " + key.name + " value to null");
            key.value = null;
            return previousIndex - 1;
        }

        /** compose the prepared query, pair.left is query id, pair.right is query */
        private Pair<Integer, String> composeQuery(String columns)
        {
            Pair<Integer, String> clause = whereClause();
            if (columns == null)
            {
                columns = "*";
            }
            else
            {
                // add keys in the front in order
                String partitionKey = keyString(partitionKeys);
                String clusterKey = keyString(clusterColumns);

                columns = withoutKeyColumns(columns);
                columns = (clusterKey == null || "".equals(clusterKey))
                        ? partitionKey + "," + columns
                        : partitionKey + "," + clusterKey + "," + columns;
            }

            return Pair.create(clause.left,
                               "SELECT " + columns
                               + " FROM " + cfName
                               + clause.right
                               + (userDefinedWhereClauses == null ? "" : " AND " + userDefinedWhereClauses)
                               + " LIMIT " + pageRowSize
                               + " ALLOW FILTERING");
        }


        /** remove key columns from the column string */
        private String withoutKeyColumns(String columnString)
        {
            Set<String> keyNames = new HashSet<String>();
            for (Key key : Iterables.concat(partitionKeys, clusterColumns))
                keyNames.add(key.name);

            String[] columns = columnString.split(",");
            String result = null;
            for (String column : columns)
            {
                String trimmed = column.trim();
                if (keyNames.contains(trimmed))
                    continue;

                result = result == null ? trimmed : result + "," + trimmed;
            }
            return result;
        }

        /** compose the where clause */
        private Pair<Integer, String> whereClause()
        {
            if (partitionKeyString == null)
                partitionKeyString = keyString(partitionKeys);

            if (partitionKeyMarkers == null)
                partitionKeyMarkers = partitionKeyMarkers();
            // initial query token(k) >= start_token and token(k) <= end_token
            if (emptyPartitionKeyValues())
                return Pair.create(0, " WHERE token(" + partitionKeyString + ") > ? AND token(" + partitionKeyString + ") <= ?");

            // query token(k) > token(pre_partition_key) and token(k) <= end_token
            if (clusterColumns.size() == 0 || clusterColumns.get(0).value == null)
                return Pair.create(1,
                                   " WHERE token(" + partitionKeyString + ") > token(" + partitionKeyMarkers + ") "
                                   + " AND token(" + partitionKeyString + ") <= ?");

            // query token(k) = token(pre_partition_key) and m = pre_cluster_key_m and n > pre_cluster_key_n
            Pair<Integer, String> clause = whereClause(clusterColumns, 0);
            return Pair.create(clause.left,
                               " WHERE token(" + partitionKeyString + ") = token(" + partitionKeyMarkers + ") " + clause.right);
        }

        /** recursively compose the where clause */
        private Pair<Integer, String> whereClause(List<Key> keys, int position)
        {
            if (position == keys.size() - 1 || keys.get(position + 1).value == null)
                return Pair.create(position + 2, " AND " + keys.get(position).name + " > ? ");

            Pair<Integer, String> clause = whereClause(keys, position + 1);
            return Pair.create(clause.left, " AND " + keys.get(position).name + " = ? " + clause.right);
        }

        /** check whether all key values are null */
        private boolean emptyPartitionKeyValues()
        {
            for (Key key : partitionKeys)
            {
                if (key.value != null)
                    return false;
            }
            return true;
        }

        /** compose the partition key string in format of <key1>, <key2>, <key3> */
        private String keyString(List<Key> keys)
        {
            String result = null;
            for (Key key : keys)
                result = result == null ? key.name : result + "," + key.name;

            return result == null ? "" : result;
        }

        /** compose the question marks for partition key string in format of ?, ? , ? */
        private String partitionKeyMarkers()
        {
            String result = null;
            for (Key key : partitionKeys)
                result = result == null ? "?" : result + ",?";

            return result;
        }

        /** compose the query binding variables, pair.left is query id, pair.right is the binding variables */
        private Pair<Integer, List<ByteBuffer>> preparedQueryBindValues()
        {
            List<ByteBuffer> values = new LinkedList<ByteBuffer>();

            // initial query token(k) >= start_token and token(k) <= end_token
            if (emptyPartitionKeyValues())
            {
                values.add(partitioner.getTokenValidator().fromString(split.getStartToken()));
                values.add(partitioner.getTokenValidator().fromString(split.getEndToken()));
                return Pair.create(0, values);
            }
            else
            {
                for (Key partitionKey1 : partitionKeys)
                    values.add(partitionKey1.value);

                if (clusterColumns.size() == 0 || clusterColumns.get(0).value == null)
                {
                    // query token(k) > token(pre_partition_key) and token(k) <= end_token
                    values.add(partitioner.getTokenValidator().fromString(split.getEndToken()));
                    return Pair.create(1, values);
                }
                else
                {
                    // query token(k) = token(pre_partition_key) and m = pre_cluster_key_m and n > pre_cluster_key_n
                    int type = preparedQueryBindValues(clusterColumns, 0, values);
                    return Pair.create(type, values);
                }
            }
        }

        /** recursively compose the query binding variables */
        private int preparedQueryBindValues(List<Key> keys, int position, List<ByteBuffer> bindValues)
        {
            if (position == keys.size() - 1 || keys.get(position + 1).value == null)
            {
                bindValues.add(keys.get(position).value);
                return position + 2;
            }
            else
            {
                bindValues.add(keys.get(position).value);
                return preparedQueryBindValues(keys, position + 1, bindValues);
            }
        }

        /**  get the prepared query item Id */
        private int prepareQuery(int type)
        {
            Integer itemId = preparedQueryIds.get(type);
            if (itemId != null)
                return itemId;

            Pair<Integer, String> query = null;
            try
            {
                query = composeQuery(columns);
                logger.debug("type:" + query.left + ", query: " + query.right);
                CqlPreparedResult cqlPreparedResult = client.prepare_cql3_query(ByteBufferUtil.bytes(query.right), Compression.NONE);
                preparedQueryIds.put(query.left, cqlPreparedResult.itemId);
                return cqlPreparedResult.itemId;
            }
            catch (InvalidRequestException e)
            {
                logger.error("failed to prepare query " + query.right, e);
            }
            catch (TException e)
            {
                logger.error("failed to prepare query " + query.right, e);
            }
            return -1;
        }

        /** execute the prepared query */
        private Iterator<CqlRow> executeQuery()
        {
            Pair<Integer, List<ByteBuffer>> bindValues = preparedQueryBindValues();
            logger.debug("query type: " + bindValues.left);

            // check whether it reach end of range for type 1 query CASSANDRA-5573
            if (bindValues.left == 1 && reachEndRange())
                return null;

            try
            {
                CqlResult cqlResult = client.execute_prepared_cql3_query(prepareQuery(bindValues.left), bindValues.right, consistencyLevel);
                if (cqlResult != null && cqlResult.rows != null)
                    return cqlResult.rows.iterator();
                else
                    return null;
            }
            catch (Exception e)
            {
                logger.error("failed to execute prepared query", e);
            }
            return null;
        }
    }

    /** retrieve the partition keys and cluster keys from system.schema_columnfamilies table */
    private void retrieveKeys() throws Exception
    {
        String query = "select key_aliases," +
                       "column_aliases, " +
                       "key_validator, " +
                       "comparator " +
                       "from system.schema_columnfamilies " +
                       "where keyspace_name='%s' and columnfamily_name='%s'";
        String formatted = String.format(query, keyspace, cfName);
        CqlResult result = client.execute_cql3_query(ByteBufferUtil.bytes(formatted), Compression.NONE, ConsistencyLevel.ONE);

        CqlRow cqlRow = result.rows.get(0);
        String keyString = ByteBufferUtil.string(ByteBuffer.wrap(cqlRow.columns.get(0).getValue()));
        logger.debug("partition keys: " + keyString);
        List<String> keys = FBUtilities.fromJsonList(keyString);

        for (String key : keys)
            partitionKeys.add(new Key(key));

        keyString = ByteBufferUtil.string(ByteBuffer.wrap(cqlRow.columns.get(1).getValue()));
        logger.debug("cluster columns: " + keyString);
        keys = FBUtilities.fromJsonList(keyString);

        for (String key : keys)
            clusterColumns.add(new Key(key));

        Column rawKeyValidator = cqlRow.columns.get(2);
        String validator = ByteBufferUtil.string(ByteBuffer.wrap(rawKeyValidator.getValue()));
        logger.debug("row key validator: " + validator);
        keyValidator = parseType(validator);

        if (keyValidator instanceof CompositeType)
        {
            List<AbstractType<?>> types = ((CompositeType) keyValidator).types;
            for (int i = 0; i < partitionKeys.size(); i++)
                partitionKeys.get(i).validator = types.get(i);
        }
        else
        {
            partitionKeys.get(0).validator = keyValidator;
        }
    }

    /** check whether current row is at the end of range */
    private boolean reachEndRange()
    {
        // current row key
        ByteBuffer rowKey;
        if (keyValidator instanceof CompositeType)
        {
            ByteBuffer[] keys = new ByteBuffer[partitionKeys.size()];
            for (int i = 0; i < partitionKeys.size(); i++)
                keys[i] = partitionKeys.get(i).value.duplicate();

            rowKey = ((CompositeType) keyValidator).build(keys);
        }
        else
        {
            rowKey = partitionKeys.get(0).value;
        }

        String endToken = split.getEndToken();
        String currentToken = partitioner.getToken(rowKey).toString();
        logger.debug("End token: " + endToken + ", current token: " + currentToken);

        return endToken.equals(currentToken);
    }

    private static AbstractType<?> parseType(String type) throws IOException
    {
        try
        {
            // always treat counters like longs, specifically CCT.compose is not what we need
            if (type != null && type.equals("org.apache.cassandra.db.marshal.CounterColumnType"))
                return LongType.instance;
            return TypeParser.parse(type);
        }
        catch (ConfigurationException e)
        {
            throw new IOException(e);
        }
        catch (SyntaxException e)
        {
            throw new IOException(e);
        }
    }

    private class Key
    {
        final String name;
        ByteBuffer value;
        AbstractType<?> validator;

        public Key(String name)
        {
            this.name = name;
        }
    }
    
    /** get string from a ByteBuffer, catch the exception and throw it as runtime exception*/
    private static String stringValue(ByteBuffer value)
    {
        try
        {
            return ByteBufferUtil.string(value);
        }
        catch (CharacterCodingException e)
        {
            throw new RuntimeException(e);
        }
    }
}
