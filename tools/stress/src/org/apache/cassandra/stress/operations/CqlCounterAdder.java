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
import java.util.Collections;
import java.util.List;

import com.yammer.metrics.core.TimerContext;
import org.apache.cassandra.db.ColumnFamilyType;
import org.apache.cassandra.stress.Session;
import org.apache.cassandra.stress.util.CassandraClient;
import org.apache.cassandra.stress.util.Operation;
import org.apache.cassandra.transport.messages.ResultMessage;
import org.apache.cassandra.thrift.Compression;
import org.apache.cassandra.thrift.CqlResult;
import org.apache.cassandra.utils.ByteBufferUtil;

public class CqlCounterAdder extends CQLOperation
{
    public CqlCounterAdder(Settings settings, int idx)
    {
        super(settings, idx);
    }

    @Override
    protected String buildQuery()
    {
        String counterCF = settings.isCql2() ? "Counter1" : "Counter3";

        StringBuilder query = new StringBuilder("UPDATE ").append(wrapInQuotesIfRequired(counterCF));

        if (settings.isCql2())
            query.append(" USING CONSISTENCY ").append(settings.consistencyLevel);

        query.append(" SET ");

        for (int i = 0; i < settings.columnsPerKey; i++)
        {
            if (i > 0)
                query.append(",");

            query.append('C').append(i).append("=C").append(i).append("+1");
        }
        query.append(" WHERE KEY=?");
        return query.toString();
    }

    @Override
    protected List<String> getQueryParameters(byte[] key)
    {
        return Collections.singletonList(getUnQuotedCqlBlob(key, settings.isCql3()));
    }

    @Override
    protected boolean validate(int rowCount)
    {
        return true;
    }

}
