/**
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
package org.apache.cassandra.stress.util;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.cassandra.db.ColumnFamilyType;
import org.apache.cassandra.stress.Session;
import org.apache.cassandra.stress.Stress;
import org.apache.cassandra.stress.StressMetrics;
import org.apache.cassandra.stress.generatedata.DataGen;
import org.apache.cassandra.stress.generatedata.DataGenGaussianHex;
import org.apache.cassandra.stress.generatedata.DataGenOpIndexToHex;
import org.apache.cassandra.stress.generatedata.DataGenRandomHex;
import org.apache.cassandra.stress.generatedata.DataGenUniform;
import org.apache.cassandra.stress.generatedata.RowGen;
import org.apache.cassandra.stress.generatedata.RowGenAverageSize;
import org.apache.cassandra.stress.generatedata.RowGenFixedSize;
import org.apache.cassandra.thrift.ColumnParent;
import org.apache.cassandra.transport.SimpleClient;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.cassandra.utils.ByteBufferUtil;

public abstract class Operation
{
    public final int index;
    protected final Settings settings;

    public Operation(Settings settings, int idx)
    {
        index = idx;
        this.settings = settings;
    }

    public static interface RunOp
    {
        public boolean run() throws Exception;
        public String key();
        public int keyCount();
    }

    public static enum ConnectionAPI
    {
        CQL,
        CQL_PREPARED,
        THRIFT
    }

    public static enum CqlVersion
    {
        CQL1, CQL2, CQL3;
        static CqlVersion get(String version)
        {
            switch(version.charAt(0))
            {
                case '1':
                    return CQL1;
                case '2':
                    return CQL2;
                case '3':
                    return CQL3;
                default:
                    throw new IllegalStateException();
            }
        }
    }

    // one per thread!
    public static final class Settings
    {

        final StressMetrics.Timer timer;
        public final Stress.Operations kind;
        public final DataGen keyGen;
        public final RowGen rowGen;
        public final ConnectionAPI connectionApi;
        public final List<ColumnParent> columnParents;
        public final StressMetrics metrics;
        public final boolean useSuperColumns;
        public final int columnsPerKey;
        public final int maxKeysAtOnce;
        public final CqlVersion cqlVersion;
        public final ConsistencyLevel consistencyLevel;
        public final int retryTimes;
        public final boolean ignoreErrors;

        // TODO : make configurable
        private final int keySize = 10;

        public final boolean useTimeUUIDComparator;
        public final boolean usePreparedStatements;
        public final List<ByteBuffer> readColumnNames;

        private final List<ByteBuffer> keyBuffers = new ArrayList<>();
        private Object cqlCache;

        public Settings(Session session, StressMetrics metrics)
        {
            this.kind = session.getOperation();
            this.timer = metrics.newTimer();
            // TODO: this logic shouldn't be here - dataGen and keyGen should be passed in
            switch (kind)
            {
                case COUNTER_ADD:
                case INSERT:
                    this.keyGen = new DataGenOpIndexToHex(session.getNumDifferentKeys());
                    break;
                default:
                    if (session.useRandomGenerator())
                        this.keyGen = new DataGenRandomHex(session.getNumDifferentKeys());
                    else
                        this.keyGen = new DataGenGaussianHex(session.getNumDifferentKeys(), session.getMean(), session.getSigma());
            }

            if (session.averageSizeValues)
                this.rowGen = new RowGenAverageSize(new DataGenUniform(session.getUniqueColumnCount()), session.getColumnsPerKey(), session.getColumnSize());
            else
                this.rowGen = new RowGenFixedSize(new DataGenUniform(session.getUniqueRowCount()), session.getColumnsPerKey(), session.getUniqueColumnCount(), session.getColumnSize());

            this.connectionApi = session.isCQL() ?
                    session.usePreparedStatements() ?
                            ConnectionAPI.CQL_PREPARED :
                            ConnectionAPI.CQL :
                    ConnectionAPI.THRIFT;
            this.metrics = metrics;
            this.cqlVersion = CqlVersion.get(session.cqlVersion);
            this.consistencyLevel = session.getConsistencyLevel();
            this.retryTimes = session.getRetryTimes();
            this.ignoreErrors = session.ignoreErrors();
            this.useTimeUUIDComparator = session.timeUUIDComparator;
            this.useSuperColumns = session.getColumnFamilyType() == ColumnFamilyType.Super;
            this.columnsPerKey = session.getColumnsPerKey();
            this.readColumnNames = session.columnNames;
            this.maxKeysAtOnce = session.getKeysPerCall();
            if (!useSuperColumns)
                columnParents = Collections.singletonList(new ColumnParent("Standard1"));
            else
            {
                ColumnParent[] cp = new ColumnParent[session.getSuperColumns()];
                for (int i = 0 ; i < cp.length ; i++)
                    cp[i] = new ColumnParent("Super1").setSuper_column(ByteBufferUtil.bytes("S" + i));
                columnParents = Arrays.asList(cp);
            }
            this.usePreparedStatements = session.usePreparedStatements();
        }
        List<ByteBuffer> getKeys(int n, int index)
        {
            while (keyBuffers.size() < n)
                keyBuffers.add(ByteBuffer.wrap(new byte[keySize]));
            keyGen.generate(keyBuffers, index);
            return keyBuffers;
        }
        public boolean isCql3()
        {
            return cqlVersion == CqlVersion.CQL3;
        }
        public boolean isCql2()
        {
            return cqlVersion == CqlVersion.CQL2;
        }
        public Object getCqlCache()
        {
            return cqlCache;
        }
        public void storeCqlCache(Object val)
        {
            cqlCache = val;
        }
    }

    protected ByteBuffer getKey()
    {
        return settings.getKeys(1, index).get(0);
    }

    protected List<ByteBuffer> getKeys(int count)
    {
        return settings.getKeys(count, index);
    }

    protected List<ByteBuffer> generateColumnValues()
    {
        return settings.rowGen.generate(index);
    }

    /**
     * Run operation
     * @param client Cassandra Thrift client connection
     * @throws IOException on any I/O error.
     */
    public abstract void run(CassandraClient client) throws IOException;

    public void run(SimpleClient client) throws IOException {
        throw new UnsupportedOperationException();
    }

    public void timeWithRetry(RunOp run) throws IOException
    {
        settings.timer.start();

        boolean success = false;
        String exceptionMessage = null;

        for (int t = 0; t < settings.retryTimes; t++)
        {
            if (success)
                break;

            try
            {
                success = run.run();
            }
            catch (Exception e)
            {
                System.err.println(e);
                exceptionMessage = getExceptionMessage(e);
                success = false;
            }
        }

        settings.timer.stop(run.keyCount());

        if (!success)
        {
            error(String.format("Operation [%d] retried %d times - error executing range slice with offset %s %s%n",
                    index,
                    settings.retryTimes,
                    run.key(),
                    (exceptionMessage == null) ? "" : "(" + exceptionMessage + ")"));
        }

    }

    protected String getExceptionMessage(Exception e)
    {
        String className = e.getClass().getSimpleName();
        String message = (e instanceof InvalidRequestException) ? ((InvalidRequestException) e).getWhy() : e.getMessage();
        return (message == null) ? "(" + className + ")" : String.format("(%s): %s", className, message);
    }

    protected void error(String message) throws IOException
    {
        if (!settings.ignoreErrors)
            throw new IOException(message);
        else
            System.err.println(message);
    }

    public static ByteBuffer getColumnName(int i)
    {
        return ByteBufferUtil.bytes("C" + i);
    }

}
