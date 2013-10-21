package org.apache.cassandra.stress.operations;
/*
 *
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
 *
 */


import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import com.yammer.metrics.core.TimerContext;
import org.apache.cassandra.db.ColumnFamilyType;
import org.apache.cassandra.stress.Session;
import org.apache.cassandra.stress.util.CassandraClient;
import org.apache.cassandra.stress.util.Operation;
import org.apache.cassandra.transport.SimpleClient;
import org.apache.cassandra.transport.messages.ResultMessage;
import org.apache.cassandra.thrift.Compression;
import org.apache.cassandra.thrift.CqlResult;
import org.apache.cassandra.thrift.ThriftConversion;

public class CqlReader extends CQLOperation
{

    public CqlReader(Settings settings, int idx)
    {
        super(settings, idx);
    }

    @Override
    protected String buildQuery()
    {
        StringBuilder query = new StringBuilder("SELECT ");

        if (settings.readColumnNames == null)
        {
            if (settings.isCql2())
                query.append("FIRST ").append(settings.columnsPerKey).append(" ''..''");
            else
                query.append("*");
        }
        else
        {
            for (int i = 0; i < settings.readColumnNames.size() ; i++)
            {
                if (i > 0)
                    query.append(",");
                query.append('?');
            }
        }

        query.append(" FROM ").append(wrapInQuotesIfRequired("Standard1"));

        if (settings.isCql2())
            query.append(" USING CONSISTENCY ").append(settings.consistencyLevel);
        query.append(" WHERE KEY=?");
        return query.toString();
    }

    @Override
    protected List<String> getQueryParameters(byte[] key)
    {
        if (settings.readColumnNames != null)
        {
            final List<String> queryParams = new ArrayList<>();
            for (ByteBuffer name : settings.readColumnNames)
                queryParams.add(getUnQuotedCqlBlob(name.array(), settings.isCql3()));
            queryParams.add(getUnQuotedCqlBlob(key, settings.isCql3()));
            return queryParams;
        }
        return Collections.singletonList(getUnQuotedCqlBlob(key, settings.isCql3()));
    }

    @Override
    protected boolean validate(int rowCount)
    {
        return rowCount != 0;
    }

}
