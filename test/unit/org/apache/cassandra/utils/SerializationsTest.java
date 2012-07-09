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
package org.apache.cassandra.utils;

import org.apache.cassandra.AbstractSerializationsTester;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.FilterFactory.Type;
import org.junit.Test;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;

public class SerializationsTest extends AbstractSerializationsTester
{

    private void testBloomFilterWrite(Type murmur) throws IOException
    {
        Filter bf = FilterFactory.getFilter(1000000, 0.0001, murmur);
        for (int i = 0; i < 100; i++)
            bf.add(StorageService.getPartitioner().getTokenFactory().toByteArray(StorageService.getPartitioner().getRandomToken()));
        DataOutput out = getRawOutput("utils.BloomFilter.bin");
        FilterFactory.serialize(bf, out, murmur);
        close();
    }

    @Test
    public void testBloomFilterReadMURMUR2() throws IOException
    {
        if (EXECUTE_WRITES)
            testBloomFilterWrite(FilterFactory.Type.MURMUR2);

        DataInput in = getRawInput("utils.BloomFilter.bin");
        assert FilterFactory.deserialize(in, FilterFactory.Type.MURMUR2) != null;
        close();
    }

    @Test
    public void testBloomFilterReadMURMUR3() throws IOException
    {
        if (EXECUTE_WRITES)
            testBloomFilterWrite(FilterFactory.Type.MURMUR3);

        DataInput in = getRawInput("utils.BloomFilter.bin");
        assert FilterFactory.deserialize(in, FilterFactory.Type.MURMUR3) != null;
        close();
    }

    private void testLegacyBloomFilterWrite() throws IOException
    {
        LegacyBloomFilter a = LegacyBloomFilter.getFilter(1000000, 1000);
        LegacyBloomFilter b = LegacyBloomFilter.getFilter(1000000, 0.0001);
        for (int i = 0; i < 100; i++)
        {
            ByteBuffer key = StorageService.getPartitioner().getTokenFactory().toByteArray(StorageService.getPartitioner().getRandomToken());
            a.add(key);
            b.add(key);
        }
        DataOutput out = getRawOutput("utils.LegacyBloomFilter.bin");
        FilterFactory.serialize(a, out, FilterFactory.Type.SHA);
        FilterFactory.serialize(b, out, FilterFactory.Type.SHA);
        close();
    }

    @Test
    public void testLegacyBloomFilterRead() throws IOException
    {
        // We never write out a new LBF.  Copy the data file from 0.7 instead.
        // if (EXECUTE_WRITES)
        //      testLegacyBloomFilterWrite();
        
        DataInput in = getRawInput("utils.LegacyBloomFilter.bin");
        assert FilterFactory.deserialize(in, FilterFactory.Type.SHA) != null;
        close();
    }

    private void testEstimatedHistogramWrite() throws IOException
    {
        EstimatedHistogram hist0 = new EstimatedHistogram();
        EstimatedHistogram hist1 = new EstimatedHistogram(5000);
        long[] offsets = new long[1000];
        long[] data = new long[offsets.length + 1];
        for (int i = 0; i < offsets.length; i++)
        {
            offsets[i] = i;
            data[i] = 10 * i;
        }
        data[offsets.length] = 100000;
        EstimatedHistogram hist2 = new EstimatedHistogram(offsets, data);

        DataOutput out = getOutput("utils.EstimatedHistogram.bin");
        EstimatedHistogram.serializer.serialize(hist0, out);
        EstimatedHistogram.serializer.serialize(hist1, out);
        EstimatedHistogram.serializer.serialize(hist2, out);
        close();
    }

    @Test
    public void testEstimatedHistogramRead() throws IOException
    {
        if (EXECUTE_WRITES)
            testEstimatedHistogramWrite();

        DataInput in = getInput("utils.EstimatedHistogram.bin");
        assert EstimatedHistogram.serializer.deserialize(in) != null;
        assert EstimatedHistogram.serializer.deserialize(in) != null;
        assert EstimatedHistogram.serializer.deserialize(in) != null;
        close();
    }
}
