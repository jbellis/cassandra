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

import org.apache.cassandra.stress.Operation;
import org.apache.cassandra.thrift.*;
import org.apache.cassandra.utils.ByteBufferUtil;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;


public final class ThriftMultiGetter extends Operation
{

    public ThriftMultiGetter(Settings settings, long index)
    {
        super(settings, index);
    }

    public void run(final Cassandra.Client client) throws IOException
    {

        final SlicePredicate predicate = new SlicePredicate().setSlice_range(new SliceRange(ByteBufferUtil.EMPTY_BYTE_BUFFER,
                ByteBufferUtil.EMPTY_BYTE_BUFFER,
                false, settings.columnsPerKey));

        final List<ByteBuffer> keys = getKeys(settings.maxKeysAtOnce);

        for (final ColumnParent parent : settings.columnParents)
        {
            timeWithRetry(new RunOp()
            {
                int count;
                @Override
                public boolean run() throws Exception
                {
                    return (count = client.multiget_slice(keys, parent, predicate, settings.consistencyLevel).size()) != 0;
                }

                @Override
                public String key()
                {
                    return keys.toString();
                }

                @Override
                public int keyCount()
                {
                    return count;
                }
            });
        }
    }

}
