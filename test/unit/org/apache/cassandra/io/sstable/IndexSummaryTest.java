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
package org.apache.cassandra.io.sstable;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;

import com.google.common.collect.Lists;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.Util;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.filter.QueryFilter;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.RandomPartitioner;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.metrics.RestorableMeter;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.Pair;

import static org.apache.cassandra.io.sstable.IndexSummaryBuilder.downsample;

import static org.apache.cassandra.io.sstable.IndexSummaryBuilder.entriesAtSamplingLevel;
import static org.junit.Assert.*;

public class IndexSummaryTest extends SchemaLoader
{
    @Test
    public void testGetKey()
    {
        Pair<List<DecoratedKey>, IndexSummary> random = generateRandomIndex(100, 1);
        for (int i = 0; i < 100; i++)
            assertEquals(random.left.get(i).key, ByteBuffer.wrap(random.right.getKey(i)));
    }

    @Test
    public void testBinarySearch()
    {
        Pair<List<DecoratedKey>, IndexSummary> random = generateRandomIndex(100, 1);
        for (int i = 0; i < 100; i++)
            assertEquals(i, random.right.binarySearch(random.left.get(i)));
    }

    @Test
    public void testGetPosition()
    {
        Pair<List<DecoratedKey>, IndexSummary> random = generateRandomIndex(100, 2);
        for (int i = 0; i < 50; i++)
            assertEquals(i*2, random.right.getPosition(i));
    }

    @Test
    public void testSerialization() throws IOException
    {
        Pair<List<DecoratedKey>, IndexSummary> random = generateRandomIndex(100, 1);
        ByteArrayOutputStream aos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(aos);
        IndexSummary.serializer.serialize(random.right, dos, false);
        // write junk
        dos.writeUTF("JUNK");
        dos.writeUTF("JUNK");
        FileUtils.closeQuietly(dos);
        DataInputStream dis = new DataInputStream(new ByteArrayInputStream(aos.toByteArray()));
        IndexSummary is = IndexSummary.serializer.deserialize(dis, DatabaseDescriptor.getPartitioner(), false);
        for (int i = 0; i < 100; i++)
            assertEquals(i, is.binarySearch(random.left.get(i)));
        // read the junk
        assertEquals(dis.readUTF(), "JUNK");
        assertEquals(dis.readUTF(), "JUNK");
        FileUtils.closeQuietly(dis);
    }

    @Test
    public void testAddEmptyKey() throws Exception
    {
        IPartitioner p = new RandomPartitioner();
        IndexSummaryBuilder builder = new IndexSummaryBuilder(1, 1, Downsampling.BASE_SAMPLING_LEVEL);
        builder.maybeAddEntry(p.decorateKey(ByteBufferUtil.EMPTY_BYTE_BUFFER), 0);
        IndexSummary summary = builder.build(p);
        assertEquals(1, summary.size());
        assertEquals(0, summary.getPosition(0));
        assertArrayEquals(new byte[0], summary.getKey(0));

        ByteArrayOutputStream aos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(aos);
        IndexSummary.serializer.serialize(summary, dos, false);
        DataInputStream dis = new DataInputStream(new ByteArrayInputStream(aos.toByteArray()));
        IndexSummary loaded = IndexSummary.serializer.deserialize(dis, p, false);

        assertEquals(1, loaded.size());
        assertEquals(summary.getPosition(0), loaded.getPosition(0));
        assertArrayEquals(summary.getKey(0), summary.getKey(0));
    }

    private Pair<List<DecoratedKey>, IndexSummary> generateRandomIndex(int size, int interval)
    {
        List<DecoratedKey> list = Lists.newArrayList();
        IndexSummaryBuilder builder = new IndexSummaryBuilder(list.size(), interval, Downsampling.BASE_SAMPLING_LEVEL);
        for (int i = 0; i < size; i++)
        {
            UUID uuid = UUID.randomUUID();
            DecoratedKey key = DatabaseDescriptor.getPartitioner().decorateKey(ByteBufferUtil.bytes(uuid));
            list.add(key);
        }
        Collections.sort(list);
        for (int i = 0; i < size; i++)
            builder.maybeAddEntry(list.get(i), i);
        IndexSummary summary = builder.build(DatabaseDescriptor.getPartitioner());
        return Pair.create(list, summary);
    }

    @Test
    public void testDownsamplePatterns()
    {
        assertEquals(Arrays.asList(0), Downsampling.getSamplingPattern(0));
        assertEquals(Arrays.asList(0), Downsampling.getSamplingPattern(1));

        assertEquals(Arrays.asList(0, 1), Downsampling.getSamplingPattern(2));
        assertEquals(Arrays.asList(0, 2, 1, 3), Downsampling.getSamplingPattern(4));
        assertEquals(Arrays.asList(0, 4, 2, 6, 1, 5, 3, 7), Downsampling.getSamplingPattern(8));
        assertEquals(Arrays.asList(0, 8, 4, 12, 2, 10, 6, 14, 1, 9, 5, 13, 3, 11, 7, 15), Downsampling.getSamplingPattern(16));
    }

    private static boolean shouldSkip(int index, List<Integer> startPoints)
    {
        for (int start : startPoints)
        {
            if ((index - start) % Downsampling.BASE_SAMPLING_LEVEL == 0)
                return true;
        }
        return false;
    }

    @Test
    public void testDownsample()
    {
        final int NUM_KEYS = 4096;
        final int INDEX_INTERVAL = 128;
        final int ORIGINAL_NUM_ENTRIES = NUM_KEYS / INDEX_INTERVAL;


        Pair<List<DecoratedKey>, IndexSummary> random = generateRandomIndex(NUM_KEYS, INDEX_INTERVAL);
        List<DecoratedKey> keys = random.left;
        IndexSummary original = random.right;

        // sanity check on the original index summary
        for (int i = 0; i < ORIGINAL_NUM_ENTRIES; i++)
            assertEquals(keys.get(i * INDEX_INTERVAL).key, ByteBuffer.wrap(original.getKey(i)));

        List<Integer> samplePattern = Downsampling.getSamplingPattern(Downsampling.BASE_SAMPLING_LEVEL);

        // downsample by one level, then two levels, then three levels...
        int downsamplingRound = 1;
        for (int samplingLevel = Downsampling.BASE_SAMPLING_LEVEL - 1; samplingLevel >= Downsampling.MIN_SAMPLING_LEVEL; samplingLevel--)
        {
            IndexSummary downsampled = downsample(original, samplingLevel, DatabaseDescriptor.getPartitioner());
            assertEquals(entriesAtSamplingLevel(original, samplingLevel), downsampled.size());

            int sampledCount = 0;
            List<Integer> skipStartPoints = samplePattern.subList(0, downsamplingRound);
            for (int i = 0; i < ORIGINAL_NUM_ENTRIES; i++)
            {
                if (!shouldSkip(i, skipStartPoints))
                {
                    assertEquals(keys.get(i * INDEX_INTERVAL).key, ByteBuffer.wrap(downsampled.getKey(sampledCount)));
                    sampledCount++;
                }
            }
            downsamplingRound++;
        }

        // downsample one level each time
        IndexSummary previous = original;
        downsamplingRound = 1;
        for (int downsampleLevel = Downsampling.BASE_SAMPLING_LEVEL - 1; downsampleLevel >= Downsampling.MIN_SAMPLING_LEVEL; downsampleLevel--)
        {
            IndexSummary downsampled = downsample(previous, downsampleLevel, DatabaseDescriptor.getPartitioner());
            assertEquals(entriesAtSamplingLevel(original, downsampleLevel), downsampled.size());

            int sampledCount = 0;
            List<Integer> skipStartPoints = samplePattern.subList(0, downsamplingRound);
            for (int i = 0; i < ORIGINAL_NUM_ENTRIES; i++)
            {
                if (!shouldSkip(i, skipStartPoints))
                {
                    assertEquals(keys.get(i * INDEX_INTERVAL).key, ByteBuffer.wrap(downsampled.getKey(sampledCount)));
                    sampledCount++;
                }
            }

            previous = downsampled;
            downsamplingRound++;
        }
    }

    @Test
    public void testRebuildAtSamplingLevel() throws IOException
    {
        String ksname = "Keyspace1";
        String cfname = "StandardLowIndexInterval";
        Keyspace keyspace = Keyspace.open(ksname);
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(cfname);
        cfs.truncateBlocking();
        cfs.disableAutoCompaction();

        ByteBuffer value = ByteBuffer.wrap(new byte[100]);

        int numRows = 256;
        for (int row = 0; row < numRows; row++)
        {
            DecoratedKey key = Util.dk(String.valueOf(row));
            RowMutation rm = new RowMutation(ksname, key.key);
            rm.add(cfname, ByteBufferUtil.bytes("column"), value, 0);
            rm.apply();
        }
        cfs.forceBlockingFlush();

        List<SSTableReader> sstrs = new ArrayList<>(cfs.getSSTables());
        assertEquals(1, sstrs.size());
        SSTableReader sstable = sstrs.get(0);

        for (int samplingLevel = Downsampling.MIN_SAMPLING_LEVEL; samplingLevel < Downsampling.BASE_SAMPLING_LEVEL; samplingLevel++)
        {
            sstable.rebuildSummary(samplingLevel);
            IndexSummary summary = sstable.getReferencedIndexSummary();
            try
            {
                assertEquals(samplingLevel, summary.getSamplingLevel());
                assertEquals((numRows * samplingLevel) / (summary.getIndexInterval() * Downsampling.BASE_SAMPLING_LEVEL), summary.size());
            }
            finally
            {
                summary.unreference();
            }
        }
    }

    private static long totalOffHeapSize(List<SSTableReader> sstables)
    {
        long total = 0;
        for (SSTableReader sstr : sstables)
            total += sstr.getIndexSummaryOffHeapSize();

        return total;
    }

    private static void resetSummaries(List<SSTableReader> sstables, long originalOffHeapSize) throws IOException
    {
        for (SSTableReader sstr : sstables)
            sstr.readMeter = new RestorableMeter(100.0, 100.0);
        org.apache.cassandra.io.sstable.IndexSummaryManager.redistributeSummaries(sstables, originalOffHeapSize * sstables.size());
        for (SSTableReader sstr : sstables)
            assertEquals(Downsampling.BASE_SAMPLING_LEVEL, sstr.getIndexSummarySamplingLevel());
    }

    private void validateData(ColumnFamilyStore cfs, int numRows)
    {
        for (int i = 0; i < numRows; i++)
        {
            DecoratedKey key = Util.dk(String.valueOf(i));
            QueryFilter filter = QueryFilter.getIdentityFilter(key, cfs.getColumnFamilyName(), System.currentTimeMillis());
            ColumnFamily row = cfs.getColumnFamily(filter);
            Column column = row.getColumn(ByteBufferUtil.bytes("column"));
            assertNotNull(column);
            assertEquals(100, column.value().array().length);
        }
    }

    @Test
    public void testRedistributeSummaries() throws IOException
    {
        String ksname = "Keyspace1";
        String cfname = "StandardLowIndexInterval";
        Keyspace keyspace = Keyspace.open(ksname);
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(cfname);
        cfs.truncateBlocking();
        cfs.disableAutoCompaction();

        ByteBuffer value = ByteBuffer.wrap(new byte[100]);

        int numSSTables = 4;
        int numRows = 256;
        for (int sstable = 0; sstable < numSSTables; sstable++)
        {
            for (int row = 0; row < numRows; row++)
            {
                DecoratedKey key = Util.dk(String.valueOf(row));
                RowMutation rm = new RowMutation(ksname, key.key);
                rm.add(cfname, ByteBufferUtil.bytes("column"), value, 0);
                rm.apply();
            }
            cfs.forceBlockingFlush();
        }

        List<SSTableReader> sstrs = new ArrayList<>(cfs.getSSTables());
        assertEquals(numSSTables, sstrs.size());
        validateData(cfs, numRows);

        for (SSTableReader sstr : sstrs)
            sstr.readMeter = new RestorableMeter(100.0, 100.0);

        long offHeapSize = sstrs.get(0).getIndexSummaryOffHeapSize();

        // there should be enough space to not downsample anything
        org.apache.cassandra.io.sstable.IndexSummaryManager.redistributeSummaries(sstrs, offHeapSize * numSSTables);
        for (SSTableReader sstr : sstrs)
            assertEquals(sstr.getIndexSummarySamplingLevel(), Downsampling.BASE_SAMPLING_LEVEL);
        assertEquals(offHeapSize * numSSTables, totalOffHeapSize(sstrs));
        validateData(cfs, numRows);

        // everything should get cut in half
        org.apache.cassandra.io.sstable.IndexSummaryManager.redistributeSummaries(sstrs, (offHeapSize * numSSTables) / 2);
        for (SSTableReader sstr : sstrs)
            assertEquals(Downsampling.BASE_SAMPLING_LEVEL / 2, sstr.getIndexSummarySamplingLevel());
        validateData(cfs, numRows);

        // everything should get cut to a quarter
        org.apache.cassandra.io.sstable.IndexSummaryManager.redistributeSummaries(sstrs, (offHeapSize * numSSTables) / 4);
        for (SSTableReader sstr : sstrs)
            assertEquals(Downsampling.BASE_SAMPLING_LEVEL / 4, sstr.getIndexSummarySamplingLevel());
        validateData(cfs, numRows);

        // upsample back up to half
        org.apache.cassandra.io.sstable.IndexSummaryManager.redistributeSummaries(sstrs, (offHeapSize * numSSTables) / 2);
        for (SSTableReader sstr : sstrs)
            assertEquals(Downsampling.BASE_SAMPLING_LEVEL / 2, sstr.getIndexSummarySamplingLevel());
        validateData(cfs, numRows);

        // upsample back up to the original index summary
        org.apache.cassandra.io.sstable.IndexSummaryManager.redistributeSummaries(sstrs, offHeapSize * numSSTables);
        for (SSTableReader sstr : sstrs)
            assertEquals(Downsampling.BASE_SAMPLING_LEVEL, sstr.getIndexSummarySamplingLevel());
        validateData(cfs, numRows);

        // make two of the four sstables cold, only leave enough space for three full index summaries,
        // so the two cold sstables should get downsampled to be half of their original size
        sstrs.get(0).readMeter = new RestorableMeter(50.0, 50.0);
        sstrs.get(1).readMeter = new RestorableMeter(50.0, 50.0);
        org.apache.cassandra.io.sstable.IndexSummaryManager.redistributeSummaries(sstrs, offHeapSize * (numSSTables - 1));
        assertEquals(Downsampling.BASE_SAMPLING_LEVEL / 2, sstrs.get(0).getIndexSummarySamplingLevel());
        assertEquals(Downsampling.BASE_SAMPLING_LEVEL / 2, sstrs.get(1).getIndexSummarySamplingLevel());
        assertEquals(Downsampling.BASE_SAMPLING_LEVEL, sstrs.get(2).getIndexSummarySamplingLevel());
        assertEquals(Downsampling.BASE_SAMPLING_LEVEL, sstrs.get(3).getIndexSummarySamplingLevel());
        validateData(cfs, numRows);

        // small increases or decreases in the read rate don't result in downsampling or upsampling
        double lowerRate = 50.0 * (org.apache.cassandra.io.sstable.IndexSummaryManager.DOWNSAMPLE_THESHOLD + (org.apache.cassandra.io.sstable.IndexSummaryManager.DOWNSAMPLE_THESHOLD * 0.10));
        double higherRate = 50.0 * (org.apache.cassandra.io.sstable.IndexSummaryManager.UPSAMPLE_THRESHOLD - (org.apache.cassandra.io.sstable.IndexSummaryManager.UPSAMPLE_THRESHOLD * 0.10));
        sstrs.get(0).readMeter = new RestorableMeter(lowerRate, lowerRate);
        sstrs.get(1).readMeter = new RestorableMeter(higherRate, higherRate);
        org.apache.cassandra.io.sstable.IndexSummaryManager.redistributeSummaries(sstrs, offHeapSize * (numSSTables - 1));
        assertEquals(Downsampling.BASE_SAMPLING_LEVEL / 2, sstrs.get(0).getIndexSummarySamplingLevel());
        assertEquals(Downsampling.BASE_SAMPLING_LEVEL / 2, sstrs.get(1).getIndexSummarySamplingLevel());
        assertEquals(Downsampling.BASE_SAMPLING_LEVEL, sstrs.get(2).getIndexSummarySamplingLevel());
        assertEquals(Downsampling.BASE_SAMPLING_LEVEL, sstrs.get(3).getIndexSummarySamplingLevel());
        validateData(cfs, numRows);

        // reset, and then this time, leave enough space for one of the cold sstables to not get downsampled
        // as far as the other
        resetSummaries(sstrs, offHeapSize);
        sstrs.get(0).readMeter = new RestorableMeter(50.0, 50.0);
        sstrs.get(1).readMeter = new RestorableMeter(50.0, 50.0);
        IndexSummary summary = sstrs.get(0).getReferencedIndexSummary();
        int extraEntries = 0;
        try
        {
            extraEntries = IndexSummaryBuilder.entriesAtSamplingLevel(summary, (Downsampling.BASE_SAMPLING_LEVEL / 2) + 1) -
                           IndexSummaryBuilder.entriesAtSamplingLevel(summary, Downsampling.BASE_SAMPLING_LEVEL / 2);
        }
        finally
        {
            summary.unreference();
        }
        double avgSize = summary.getOffHeapSize() / (double) summary.size();
        int extraSpace = (int) Math.ceil(extraEntries * avgSize);
        org.apache.cassandra.io.sstable.IndexSummaryManager.redistributeSummaries(sstrs, offHeapSize * (numSSTables - 1) + extraSpace);

        if (sstrs.get(0).getIndexSummarySamplingLevel() == Downsampling.BASE_SAMPLING_LEVEL / 2)
            assertEquals((Downsampling.BASE_SAMPLING_LEVEL / 2) + 1, sstrs.get(1).getIndexSummarySamplingLevel());
        else if (sstrs.get(1).getIndexSummarySamplingLevel() == Downsampling.BASE_SAMPLING_LEVEL / 2)
            assertEquals((Downsampling.BASE_SAMPLING_LEVEL / 2) + 1, sstrs.get(0).getIndexSummarySamplingLevel());
        else
            fail("One of the first two sstables should be downsampled to half of the original summary");

        assertEquals(Downsampling.BASE_SAMPLING_LEVEL, sstrs.get(2).getIndexSummarySamplingLevel());
        assertEquals(Downsampling.BASE_SAMPLING_LEVEL, sstrs.get(3).getIndexSummarySamplingLevel());
        validateData(cfs, numRows);

        // Cause a mix of upsampling and downsampling. We'll leave enough space for two full index summaries. The two
        // coldest sstables will get downsampled to 1/4 of their size, leaving us with 1.5 index summaries worth of
        // space.  The hottest sstable should get a full index summary, and the one in the middle should get half
        // a summary.
        sstrs.get(0).readMeter = new RestorableMeter(200.0, 200.0);
        sstrs.get(1).readMeter = new RestorableMeter(100.0, 100.0);
        sstrs.get(2).readMeter = new RestorableMeter(0.0, 0.0);
        sstrs.get(3).readMeter = new RestorableMeter(0.0, 0.0);
        org.apache.cassandra.io.sstable.IndexSummaryManager.redistributeSummaries(sstrs, offHeapSize * 2);
        assertEquals(Downsampling.BASE_SAMPLING_LEVEL, sstrs.get(0).getIndexSummarySamplingLevel());
        assertEquals(Downsampling.BASE_SAMPLING_LEVEL / 2, sstrs.get(1).getIndexSummarySamplingLevel());
        assertEquals(Downsampling.MIN_SAMPLING_LEVEL, sstrs.get(2).getIndexSummarySamplingLevel());
        assertEquals(Downsampling.MIN_SAMPLING_LEVEL, sstrs.get(3).getIndexSummarySamplingLevel());
        validateData(cfs, numRows);

        // Don't leave enough space for even the minimal index summaries
        org.apache.cassandra.io.sstable.IndexSummaryManager.redistributeSummaries(sstrs, offHeapSize - 100);
        for (SSTableReader sstr : sstrs)
            assertEquals(Downsampling.MIN_SAMPLING_LEVEL, sstr.getIndexSummarySamplingLevel());
        validateData(cfs, numRows);
    }

    @Test
    public void testOriginalIndexLookup()
    {
        for (int i = Downsampling.BASE_SAMPLING_LEVEL; i >= Downsampling.MIN_SAMPLING_LEVEL; i--)
            assertEquals(i, Downsampling.getOriginalIndexes(i).size());

        // spot check a few values (these depend on BASE_SAMPLING_LEVEL being 16)
        assert Downsampling.BASE_SAMPLING_LEVEL == 16;
        assertEquals(Arrays.asList(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15), Downsampling.getOriginalIndexes(16));
        assertEquals(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15), Downsampling.getOriginalIndexes(15));
        assertEquals(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 9, 10, 11, 12, 13, 14, 15), Downsampling.getOriginalIndexes(14));

        assertEquals(Arrays.asList(1, 3, 5, 7, 9, 11, 13, 15), Downsampling.getOriginalIndexes(8));
        assertEquals(Arrays.asList(3, 7, 11, 15), Downsampling.getOriginalIndexes(4));
        assertEquals(Arrays.asList(7, 15), Downsampling.getOriginalIndexes(2));
        assertEquals(Arrays.asList(), Downsampling.getOriginalIndexes(0));
    }

    @Test
    public void testGetNumberOfSkippedEntriesAfterIndex()
    {
        int indexInterval = 128;
        for (int i = 0; i < Downsampling.BASE_SAMPLING_LEVEL; i++)
            assertEquals(indexInterval, Downsampling.getEffectiveIndexIntervalAfterIndex(i, Downsampling.BASE_SAMPLING_LEVEL, indexInterval));

        // with one round of downsampling, only the first summary has been removed, so only the last index will have
        // double the gap until the next sample
        for (int i = 0; i < Downsampling.BASE_SAMPLING_LEVEL - 2; i++)
            assertEquals(indexInterval, Downsampling.getEffectiveIndexIntervalAfterIndex(i, Downsampling.BASE_SAMPLING_LEVEL - 1, indexInterval));
        assertEquals(indexInterval * 2, Downsampling.getEffectiveIndexIntervalAfterIndex(Downsampling.BASE_SAMPLING_LEVEL - 2, Downsampling.BASE_SAMPLING_LEVEL - 1, indexInterval));

        // at samplingLevel=2, the retained summary points are [7, 15] (assumes BASE_SAMPLING_LEVEL is 16)
        assert Downsampling.BASE_SAMPLING_LEVEL == 16;
        assertEquals(8 * indexInterval, Downsampling.getEffectiveIndexIntervalAfterIndex(0, 2, indexInterval));
        assertEquals(8 * indexInterval, Downsampling.getEffectiveIndexIntervalAfterIndex(1, 2, indexInterval));
    }
}