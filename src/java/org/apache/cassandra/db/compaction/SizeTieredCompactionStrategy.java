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
package org.apache.cassandra.db.compaction;

import java.util.*;
import java.util.Map.Entry;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Iterables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.cql3.CFPropDefs;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.io.sstable.SSTableReader;
import org.apache.cassandra.utils.Pair;

public class SizeTieredCompactionStrategy extends AbstractCompactionStrategy
{
    private static final Logger logger = LoggerFactory.getLogger(SizeTieredCompactionStrategy.class);

    protected SizeTieredCompactionStrategyOptions options;
    protected volatile int estimatedRemainingTasks;

    public SizeTieredCompactionStrategy(ColumnFamilyStore cfs, Map<String, String> options)
    {
        super(cfs, options);
        this.estimatedRemainingTasks = 0;
        this.options = new SizeTieredCompactionStrategyOptions(options);
    }

    private List<SSTableReader> getNextBackgroundSSTables(final int gcBefore)
    {
        if (!isEnabled())
            return Collections.emptyList();

        // make local copies so they can't be changed out from under us mid-method
        int minThreshold = cfs.getMinimumCompactionThreshold();
        int maxThreshold = cfs.getMaximumCompactionThreshold();

        Set<SSTableReader> candidates = cfs.getUncompactingSSTables();
        List<List<SSTableReader>> buckets = getBuckets(createSSTableAndLengthPairs(filterSuspectSSTables(candidates)), options.bucketHigh, options.bucketLow, options.minSSTableSize);
        logger.debug("Compaction buckets are {}", buckets);
        updateEstimatedCompactionsByTasks(buckets);
        List<SSTableReader> mostInteresting = mostInterestingBucket(buckets, minThreshold, maxThreshold, options.coldnessThreshold);
        if (!mostInteresting.isEmpty())
            return mostInteresting;

        // if there is no sstable to compact in standard way, try compacting single sstable whose droppable tombstone
        // ratio is greater than threshold.
        List<SSTableReader> sstablesWithTombstones = new ArrayList<SSTableReader>();
        for (SSTableReader sstable : candidates)
        {
            if (worthDroppingTombstones(sstable, gcBefore))
                sstablesWithTombstones.add(sstable);
        }
        if (sstablesWithTombstones.isEmpty())
            return Collections.emptyList();

        Collections.sort(sstablesWithTombstones, new SSTableReader.SizeComparator());
        return Collections.singletonList(sstablesWithTombstones.get(0));
    }

    /**
     * @param buckets list of buckets from which to return the most interesting, where "interesting" is the total hotness for reads
     * @param minThreshold minimum number of sstables in a bucket to qualify as interesting
     * @param maxThreshold maximum number of sstables to compact at once (the returned bucket will be trimmed down to this)
     * @param coldnessThreshold coldness relative to other sstables *in the same bucket* to qualify as interesting; sstables below the threshold will be omitted
     * @return a bucket (list) of sstables to compact
     */
    public static List<SSTableReader> mostInterestingBucket(List<List<SSTableReader>> buckets, int minThreshold, int maxThreshold, double coldnessThreshold)
    {
        // skip buckets containing less than minThreshold sstables, and limit other buckets to maxThreshold entries
        final List<Pair<List<SSTableReader>, Double>> prunedBucketsAndHotness = new ArrayList<>(buckets.size());
        for (List<SSTableReader> bucket : buckets)
        {
            Pair<List<SSTableReader>, Double> bucketAndHotness = trimToThresholdWithHotness(bucket, maxThreshold, coldnessThreshold);
            if (bucketAndHotness != null && bucketAndHotness.left.size() >= minThreshold)
                prunedBucketsAndHotness.add(bucketAndHotness);
        }
        if (prunedBucketsAndHotness.isEmpty())
            return Collections.emptyList();

        // prefer compacting the hottest bucket
        Pair<List<SSTableReader>, Double> hottest = Collections.max(prunedBucketsAndHotness, new Comparator<Pair<List<SSTableReader>, Double>>()
        {
            public int compare(Pair<List<SSTableReader>, Double> o1, Pair<List<SSTableReader>, Double> o2)
            {
                int comparison = Double.compare(o1.right, o2.right);
                if (comparison != 0)
                    return comparison;

                // break ties by compacting the smallest sstables first (this will probably only happen for
                // system tables and new/unread sstables)
                return Long.compare(avgSize(o1.left), avgSize(o2.left));
            }

            private long avgSize(List<SSTableReader> sstables)
            {
                long n = 0;
                for (SSTableReader sstable : sstables)
                    n += sstable.bytesOnDisk();
                return n / sstables.size();
            }
        });

        return hottest.left;
    }

    /**
     * Removes cold sstables from the bucket and returns a (bucket, hotness) pair or null if there were not enough
     * hot sstables in the bucket to meet minThreshold.
     */
    @VisibleForTesting
    static Pair<List<SSTableReader>, Double> trimToThresholdWithHotness(List<SSTableReader> bucket, int maxThreshold, double coldnessThreshold)
    {
        // calculate the average SSTable hotness across the bucket
        double averageSSTableHotness = 0.0;
        for (SSTableReader sstr : bucket)
            averageSSTableHotness += hotness(sstr);
        averageSSTableHotness /= bucket.size();

        // sort by sstable hotness (descending)
        Collections.sort(bucket, new Comparator<SSTableReader>()
        {
            public int compare(SSTableReader o1, SSTableReader o2)
            {
                return -1 * Double.compare(hotness(o1), hotness(o2));
            }
        });

        // remove SSTables that are below the configured relative hotness and calculate the full bucket hotness
        // (the sum of the hotness of all sstable members)
        int bucketEndIndex = 0;
        double bucketHotness = 0.0;
        for (SSTableReader sstr : bucket)
        {
            if (bucketEndIndex >= maxThreshold)
                break;

            double hotness = hotness(sstr);
            if (hotness < coldnessThreshold * averageSSTableHotness)
                break; // because they're sorted, all sstables after this will be colder

            bucketEndIndex++;
            bucketHotness += hotness;
        }

        // cut off the bucket where it becomes cold or at the maxThreshold, whichever is lower
        return Pair.create(bucket.subList(0, bucketEndIndex), bucketHotness);
    }

    private static double hotness(SSTableReader sstr)
    {
        // system tables don't have read meters, just use 0.0 for the hotness
        return sstr.readMeter == null ? 0.0 : sstr.readMeter.twoHourRate() / sstr.estimatedKeys();
    }

    public synchronized AbstractCompactionTask getNextBackgroundTask(int gcBefore)
    {
        if (!isEnabled())
            return null;

        while (true)
        {
            List<SSTableReader> hottestBucket = getNextBackgroundSSTables(gcBefore);

            if (hottestBucket.isEmpty())
                return null;

            if (cfs.getDataTracker().markCompacting(hottestBucket))
                return new CompactionTask(cfs, hottestBucket, gcBefore);
        }
    }

    public AbstractCompactionTask getMaximalTask(final int gcBefore)
    {
        Iterable<SSTableReader> sstables = cfs.markAllCompacting();
        if (sstables == null)
            return null;

        return new CompactionTask(cfs, sstables, gcBefore);
    }

    public AbstractCompactionTask getUserDefinedTask(Collection<SSTableReader> sstables, final int gcBefore)
    {
        assert !sstables.isEmpty(); // checked for by CM.submitUserDefined

        if (!cfs.getDataTracker().markCompacting(sstables))
        {
            logger.debug("Unable to mark {} for compaction; probably a background compaction got to it first.  You can disable background compactions temporarily if this is a problem", sstables);
            return null;
        }

        return new CompactionTask(cfs, sstables, gcBefore).setUserDefined(true);
    }

    public int getEstimatedRemainingTasks()
    {
        return estimatedRemainingTasks;
    }

    public static List<Pair<SSTableReader, Long>> createSSTableAndLengthPairs(Iterable<SSTableReader> sstables)
    {
        List<Pair<SSTableReader, Long>> sstableLengthPairs = new ArrayList<Pair<SSTableReader, Long>>(Iterables.size(sstables));
        for(SSTableReader sstable : sstables)
            sstableLengthPairs.add(Pair.create(sstable, sstable.onDiskLength()));
        return sstableLengthPairs;
    }

    /*
     * Group files of similar size into buckets.
     */
    public static <T> List<List<T>> getBuckets(Collection<Pair<T, Long>> files, double bucketHigh, double bucketLow, long minSSTableSize)
    {
        // Sort the list in order to get deterministic results during the grouping below
        List<Pair<T, Long>> sortedFiles = new ArrayList<Pair<T, Long>>(files);
        Collections.sort(sortedFiles, new Comparator<Pair<T, Long>>()
        {
            public int compare(Pair<T, Long> p1, Pair<T, Long> p2)
            {
                return p1.right.compareTo(p2.right);
            }
        });

        Map<Long, List<T>> buckets = new HashMap<Long, List<T>>();

        outer:
        for (Pair<T, Long> pair: sortedFiles)
        {
            long size = pair.right;

            // look for a bucket containing similar-sized files:
            // group in the same bucket if it's w/in 50% of the average for this bucket,
            // or this file and the bucket are all considered "small" (less than `minSSTableSize`)
            for (Entry<Long, List<T>> entry : buckets.entrySet())
            {
                List<T> bucket = entry.getValue();
                long oldAverageSize = entry.getKey();
                if ((size > (oldAverageSize * bucketLow) && size < (oldAverageSize * bucketHigh))
                    || (size < minSSTableSize && oldAverageSize < minSSTableSize))
                {
                    // remove and re-add under new new average size
                    buckets.remove(oldAverageSize);
                    long totalSize = bucket.size() * oldAverageSize;
                    long newAverageSize = (totalSize + size) / (bucket.size() + 1);
                    bucket.add(pair.left);
                    buckets.put(newAverageSize, bucket);
                    continue outer;
                }
            }

            // no similar bucket found; put it in a new one
            ArrayList<T> bucket = new ArrayList<T>();
            bucket.add(pair.left);
            buckets.put(size, bucket);
        }

        return new ArrayList<List<T>>(buckets.values());
    }

    private void updateEstimatedCompactionsByTasks(List<List<SSTableReader>> tasks)
    {
        int n = 0;
        for (List<SSTableReader> bucket: tasks)
        {
            if (bucket.size() >= cfs.getMinimumCompactionThreshold())
                n += Math.ceil((double)bucket.size() / cfs.getMaximumCompactionThreshold());
        }
        estimatedRemainingTasks = n;
    }

    public long getMaxSSTableSize()
    {
        return Long.MAX_VALUE;
    }

    public static Map<String, String> validateOptions(Map<String, String> options) throws ConfigurationException
    {
        Map<String, String> uncheckedOptions = AbstractCompactionStrategy.validateOptions(options);
        uncheckedOptions = SizeTieredCompactionStrategyOptions.validateOptions(options, uncheckedOptions);

        uncheckedOptions.remove(CFPropDefs.KW_MINCOMPACTIONTHRESHOLD);
        uncheckedOptions.remove(CFPropDefs.KW_MAXCOMPACTIONTHRESHOLD);

        return uncheckedOptions;
    }

    public String toString()
    {
        return String.format("SizeTieredCompactionStrategy[%s/%s]",
            cfs.getMinimumCompactionThreshold(),
            cfs.getMaximumCompactionThreshold());
    }
}