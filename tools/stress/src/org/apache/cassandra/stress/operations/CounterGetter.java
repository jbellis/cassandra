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

import com.yammer.metrics.core.TimerContext;
import org.apache.cassandra.stress.Session;
import org.apache.cassandra.stress.util.CassandraClient;
import org.apache.cassandra.stress.util.Operation;
import org.apache.cassandra.db.ColumnFamilyType;
import org.apache.cassandra.thrift.*;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

public class CounterGetter extends Operation
{
    public CounterGetter(Settings settings, long index)
    {
        super(settings, index);
    }

    public void run(final CassandraClient client) throws IOException
    {
        SliceRange sliceRange = new SliceRange();
        // start/finish
        sliceRange.setStart(new byte[] {}).setFinish(new byte[] {});
        // reversed/count
        sliceRange.setReversed(false).setCount(settings.columnsPerKey);
        // initialize SlicePredicate with existing SliceRange
        final SlicePredicate predicate = new SlicePredicate().setSlice_range(sliceRange);

        final ByteBuffer key = getKey();
        for (final ColumnParent parent : settings.columnParents)
        {

            timeWithRetry(new RunOp()
            {
                @Override
                public boolean run() throws Exception
                {
                    return client.get_slice(key, parent, predicate, settings.consistencyLevel).size() != 0;
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
    }

}
