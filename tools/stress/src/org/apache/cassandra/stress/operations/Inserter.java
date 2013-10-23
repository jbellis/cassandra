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
package org.apache.cassandra.stress.operations;

import org.apache.cassandra.db.marshal.TimeUUIDType;
import org.apache.cassandra.stress.util.CassandraClient;
import org.apache.cassandra.stress.util.Operation;
import org.apache.cassandra.thrift.*;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.UUIDGen;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;

public final class Inserter extends Operation
{

    public Inserter(Settings settings, long index)
    {
        super(settings, index);
    }

    public void run(final CassandraClient client) throws IOException
    {
        final ByteBuffer key = getKey();
        final List<Column> columns = generateColumns();

        Map<String, List<Mutation>> row;
        if (!settings.useSuperColumns)
        {
            List<Mutation> mutations = new ArrayList<>(columns.size());
            for (Column c : columns)
            {
                ColumnOrSuperColumn column = new ColumnOrSuperColumn().setColumn(c);
                mutations.add(new Mutation().setColumn_or_supercolumn(column));
            }
            row = Collections.singletonMap("Standard1", mutations);
        }
        else
        {
            List<Mutation> mutations = new ArrayList<>(settings.columnParents.size());
            for (ColumnParent parent : settings.columnParents)
            {
                final SuperColumn s = new SuperColumn(parent.bufferForSuper_column(), columns);
                final ColumnOrSuperColumn cosc = new ColumnOrSuperColumn().setSuper_column(s);
                mutations.add(new Mutation().setColumn_or_supercolumn(cosc));
            }
            row = Collections.singletonMap("Super1", mutations);
        }

        final Map<ByteBuffer, Map<String, List<Mutation>>> record = Collections.singletonMap(key, row);

        timeWithRetry(new RunOp()
        {
            @Override
            public boolean run() throws Exception
            {
                client.batch_mutate(record, settings.consistencyLevel);
                return true;
            }

            @Override
            public String key()
            {
                return new String(key.array());
            }

            @Override
            public int keyCount()
            {
                return 1;
            }
        });
    }

    protected List<Column> generateColumns()
    {
        final List<ByteBuffer> values = generateColumnValues();
        final List<Column> columns = new ArrayList<>(settings.columnsPerKey);
        assert columns.size() == values.size();

        // initialise columns to use string names, then override if want timeUUID
        // on every call, as they need to be written each time anyway
        if (columns.isEmpty())
            for (int i = 0 ; i < values.size() ; i++)
                columns.add(new Column(getColumnName(i)));

        if (settings.useTimeUUIDComparator)
            for (Column column : columns)
                column.setName(TimeUUIDType.instance.decompose(UUIDGen.getTimeUUID()));

        for (int i = 0 ; i < values.size() ; i++)
            columns.get(i)
                    .setValue(values.get(i))
                    .setTimestamp(FBUtilities.timestampMicros());

        return columns;
    }

}
