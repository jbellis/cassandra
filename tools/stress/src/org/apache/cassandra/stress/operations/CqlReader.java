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


import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class CqlReader extends CqlOperation
{

    public CqlReader(State state, long idx)
    {
        super(state, idx);
    }

    @Override
    protected String buildQuery()
    {
        StringBuilder query = new StringBuilder("SELECT ");

        if (state.settings.columns.names == null)
        {
            if (state.isCql2())
                query.append("FIRST ").append(state.settings.columns.maxColumnsPerKey).append(" ''..''");
            else
                query.append("*");
        }
        else
        {
            for (int i = 0; i < state.settings.columns.names.size() ; i++)
            {
                if (i > 0)
                    query.append(",");
                query.append('?');
            }
        }

        query.append(" FROM ").append(wrapInQuotesIfRequired(state.settings.schema.columnFamily));

        if (state.isCql2())
            query.append(" USING CONSISTENCY ").append(state.settings.op.consistencyLevel);
        query.append(" WHERE KEY=?");
        return query.toString();
    }

    @Override
    protected List<String> getQueryParameters(byte[] key)
    {
        if (state.settings.columns.names != null)
        {
            final List<String> queryParams = new ArrayList<>();
            for (ByteBuffer name : state.settings.columns.names)
                queryParams.add(getUnQuotedCqlBlob(name.array(), state.isCql3()));
            queryParams.add(getUnQuotedCqlBlob(key, state.isCql3()));
            return queryParams;
        }
        return Collections.singletonList(getUnQuotedCqlBlob(key, state.isCql3()));
    }

    @Override
    protected boolean validate(int rowCount)
    {
        return rowCount != 0;
    }

}
