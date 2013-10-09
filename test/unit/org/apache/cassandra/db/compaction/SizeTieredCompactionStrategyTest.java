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
package org.apache.cassandra.db.compaction;

import java.nio.ByteBuffer;
import java.util.*;

import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.Util;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.RowMutation;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.io.sstable.SSTableReader;
import org.apache.cassandra.metrics.RestorableMeter;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.Pair;

import static org.junit.Assert.*;

public class SizeTieredCompactionStrategyTest extends SchemaLoader
{

    @Test
    public void testOptionsValidation() throws ConfigurationException
    {
        Map<String, String> options = new HashMap<>();
        options.put(SizeTieredCompactionStrategyOptions.COLDNESS_THRESHOLD_KEY, "0.35");
        options.put(SizeTieredCompactionStrategyOptions.BUCKET_LOW_KEY, "0.5");
        options.put(SizeTieredCompactionStrategyOptions.BUCKET_HIGH_KEY, "1.5");
        options.put(SizeTieredCompactionStrategyOptions.MIN_SSTABLE_SIZE_KEY, "10000");
        Map<String, String> unvalidated = SizeTieredCompactionStrategy.validateOptions(options);
        assertTrue(unvalidated.isEmpty());

        try
        {
            options.put(SizeTieredCompactionStrategyOptions.COLDNESS_THRESHOLD_KEY, "-0.5");
            SizeTieredCompactionStrategy.validateOptions(options);
            fail("Negative coldness_threshold should be rejected");
        }
        catch (ConfigurationException e)
        {
            options.put(SizeTieredCompactionStrategyOptions.COLDNESS_THRESHOLD_KEY, "0.25");
        }

        try
        {
            options.put(SizeTieredCompactionStrategyOptions.BUCKET_LOW_KEY, "1000.0");
            SizeTieredCompactionStrategy.validateOptions(options);
            fail("bucket_low greater than bucket_high should be rejected");
        }
        catch (ConfigurationException e)
        {
            options.put(SizeTieredCompactionStrategyOptions.BUCKET_LOW_KEY, "0.5");
        }

        options.put("bad_option", "1.0");
        unvalidated = SizeTieredCompactionStrategy.validateOptions(options);
        assertTrue(unvalidated.containsKey("bad_option"));
    }

    @Test
    public void testGetBuckets()
    {
        List<Pair<String, Long>> pairs = new ArrayList<Pair<String, Long>>();
        String[] strings = { "a", "bbbb", "cccccccc", "cccccccc", "bbbb", "a" };
        for (String st : strings)
        {
            Pair<String, Long> pair = Pair.create(st, new Long(st.length()));
            pairs.add(pair);
        }

        List<List<String>> buckets = SizeTieredCompactionStrategy.getBuckets(pairs, 1.5, 0.5, 2);
        assertEquals(3, buckets.size());

        for (List<String> bucket : buckets)
        {
            assertEquals(2, bucket.size());
            assertEquals(bucket.get(0).length(), bucket.get(1).length());
            assertEquals(bucket.get(0).charAt(0), bucket.get(1).charAt(0));
        }

        pairs.clear();
        buckets.clear();

        String[] strings2 = { "aaa", "bbbbbbbb", "aaa", "bbbbbbbb", "bbbbbbbb", "aaa" };
        for (String st : strings2)
        {
            Pair<String, Long> pair = Pair.create(st, new Long(st.length()));
            pairs.add(pair);
        }

        buckets = SizeTieredCompactionStrategy.getBuckets(pairs, 1.5, 0.5, 2);
        assertEquals(2, buckets.size());

        for (List<String> bucket : buckets)
        {
            assertEquals(3, bucket.size());
            assertEquals(bucket.get(0).charAt(0), bucket.get(1).charAt(0));
            assertEquals(bucket.get(1).charAt(0), bucket.get(2).charAt(0));
        }

        // Test the "min" functionality
        pairs.clear();
        buckets.clear();

        String[] strings3 = { "aaa", "bbbbbbbb", "aaa", "bbbbbbbb", "bbbbbbbb", "aaa" };
        for (String st : strings3)
        {
            Pair<String, Long> pair = Pair.create(st, new Long(st.length()));
            pairs.add(pair);
        }

        buckets = SizeTieredCompactionStrategy.getBuckets(pairs, 1.5, 0.5, 10);
        assertEquals(1, buckets.size());
    }

    @Test
    public void testPrepBucket() throws Exception
    {
        String ksname = "Keyspace1";
        String cfname = "Standard1";
        Keyspace keyspace = Keyspace.open(ksname);
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(cfname);
        cfs.disableAutoCompaction();

        ByteBuffer value = ByteBuffer.wrap(new byte[100]);

        // create 10 sstables
        int numSSTables = 10;
        for (int r = 0; r < numSSTables; r++)
        {
            DecoratedKey key = Util.dk(String.valueOf(r));
            RowMutation rm = new RowMutation(ksname, key.key);
            rm.add(cfname, ByteBufferUtil.bytes("column"), value, 0);
            rm.apply();
            cfs.forceBlockingFlush();
        }
        cfs.forceBlockingFlush();
        SizeTieredCompactionStrategy strategy = (SizeTieredCompactionStrategy) cfs.getCompactionStrategy();

        List<SSTableReader> sstrs = new ArrayList<>(cfs.getSSTables());
        Pair<List<SSTableReader>, Double> bucket;

        bucket = strategy.prepBucket(sstrs.subList(0, 2), 4, 32, 0.25);
        assertNull("null should be returned when the bucket is below the min threshold", bucket);

        long estimatedKeys = sstrs.get(0).estimatedKeys();

        // make all the buckets equal, none should get dropped
        for (SSTableReader s : sstrs)
            s.readMeter = new RestorableMeter(100.0, 100.0);

        bucket = strategy.prepBucket(sstrs, 4, 32, 0.25);
        assertEquals("no buckets should be dropped", 10, bucket.left.size());
        double expectedBucketHotness = numSSTables * (100.0 / estimatedKeys);
        assertEquals(String.format("bucket hotness (%f) should be close to %f", bucket.right, expectedBucketHotness),
                     expectedBucketHotness, bucket.right, 1.0);

        // all of the buckets are hot enough to be kept, but we have two more than the maxThreshold, so the two coldest
        // should get dropped
        sstrs.get(0).readMeter = new RestorableMeter(90.0, 90.0);
        sstrs.get(1).readMeter = new RestorableMeter(90.0, 90.0);
        bucket = strategy.prepBucket(sstrs, 4, numSSTables - 2, 0.25);
        assertEquals("two buckets should be dropped", numSSTables - 2, bucket.left.size());
        expectedBucketHotness = (numSSTables - 2) * (100.0 / estimatedKeys);
        assertEquals(String.format("bucket hotness (%f) should be close to %f", bucket.right, expectedBucketHotness),
                     expectedBucketHotness, bucket.right, 1.0);

        // two of the buckets are cold enough to be excluded
        sstrs.get(0).readMeter = new RestorableMeter(10.0, 10.0);
        sstrs.get(1).readMeter = new RestorableMeter(10.0, 10.0);
        bucket = strategy.prepBucket(sstrs, 4, 32, 0.25);
        assertEquals("two buckets should be dropped", 8, bucket.left.size());
        expectedBucketHotness = (numSSTables - 2) * (100.0 / estimatedKeys);
        assertEquals(String.format("bucket hotness (%f) should be close to %f", bucket.right, expectedBucketHotness),
                     expectedBucketHotness, bucket.right, 1.0);

        // two of the buckets are cold enough to be excluded, but this will drop us below the min threshold
        bucket = strategy.prepBucket(sstrs, numSSTables - 1, 32, 0.25);
        assertNull("null should be returned when the bucket drops below the min threshold", bucket);
    }
}
