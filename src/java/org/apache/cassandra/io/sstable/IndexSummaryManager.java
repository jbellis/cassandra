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

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.util.*;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import javax.management.MBeanServer;
import javax.management.ObjectName;

import com.google.common.annotations.VisibleForTesting;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.concurrent.DebuggableScheduledThreadPoolExecutor;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.metrics.RestorableMeter;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.WrappedRunnable;

/**
 * Manages the fixed-size memory pool for index summaries, periodically resizing them
 * in order to give more memory to hot sstables and less memory to cold sstables.
 */
public class IndexSummaryManager implements IndexSummaryManagerMBean
{
    private static final Logger logger = LoggerFactory.getLogger(IndexSummaryManager.class);
    public static final String MBEAN_NAME = "org.apache.cassandra.db:type=IndexSummaries";
    public static final IndexSummaryManager instance;

    private int resizeIntervalInMinutes = 0;
    private long memoryPoolBytes;

    // The target (or ideal) number of index summary entries must differ from the actual number of
    // entries by this ratio in order to trigger an upsample or downsample of the summary.  Because
    // upsampling requires reading the primary index in order to rebuild the summary, the threshold
    // for upsampling is is higher.
    static final double UPSAMPLE_THRESHOLD = 1.5;
    static final double DOWNSAMPLE_THESHOLD = 0.9;

    private final DebuggableScheduledThreadPoolExecutor executor;

    // our next scheduled resizing run
    private ScheduledFuture future;

    static
    {
        instance = new IndexSummaryManager();
        MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();

        try
        {
            mbs.registerMBean(instance, new ObjectName(MBEAN_NAME));
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }
    }

    private IndexSummaryManager()
    {
        executor = new DebuggableScheduledThreadPoolExecutor(1, "IndexSummaryManager", Thread.MIN_PRIORITY);

        long indexSummarySizeInMB = DatabaseDescriptor.getIndexSummaryCapacityInMB();
        int interval = DatabaseDescriptor.getIndexSummaryResizeIntervalInMinutes();
        logger.info("Initializing index summary manager with a memory pool size of {} MB and a resize interval of {} minutes",
                    indexSummarySizeInMB, interval);

        setMemoryPoolCapacityInMB(DatabaseDescriptor.getIndexSummaryCapacityInMB());
        setResizeIntervalInMinutes(DatabaseDescriptor.getIndexSummaryResizeIntervalInMinutes());
    }

    public int getResizeIntervalInMinutes()
    {
        return resizeIntervalInMinutes;
    }

    public void setResizeIntervalInMinutes(int resizeIntervalInMinutes)
    {
        int difference = resizeIntervalInMinutes - this.resizeIntervalInMinutes;
        this.resizeIntervalInMinutes = resizeIntervalInMinutes;

        long initialDelay;
        if (future != null)
        {
            long remaining = future.getDelay(TimeUnit.MINUTES);
            initialDelay = Math.max(0, remaining + difference);
            future.cancel(false);
        }
        else
        {
            initialDelay = this.resizeIntervalInMinutes;
        }

        if (this.resizeIntervalInMinutes < 0)
        {
            future = null;
            return;
        }

        future = executor.scheduleWithFixedDelay(new WrappedRunnable()
        {
            protected void runMayThrow() throws Exception
            {
                redistributeSummaries();
            }
        }, initialDelay, resizeIntervalInMinutes, TimeUnit.MINUTES);
    }

    public long getMemoryPoolCapacityInMB()
    {
        return memoryPoolBytes / 1024L / 1024L;
    }

    public Map<String, Double> getSamplingRatios()
    {
        List<SSTableReader> sstables = getAllNoncompactingSSTables();
        Map<String, Double> ratios = new HashMap<>(sstables.size());
        for (SSTableReader sstable : sstables)
            ratios.put(sstable.getFilename(), sstable.getIndexSummarySamplingLevel() / (double) Downsampling.BASE_SAMPLING_LEVEL);
        return ratios;
    }

    public double getAverageSamplingRatio()
    {
        List<SSTableReader> sstables = getAllNoncompactingSSTables();
        double total = 0.0;
        for (SSTableReader sstable : sstables)
            total += sstable.getIndexSummarySamplingLevel() / (double) Downsampling.BASE_SAMPLING_LEVEL;
        return total / sstables.size();
    }

    public void setMemoryPoolCapacityInMB(long memoryPoolCapacityInMB)
    {
        this.memoryPoolBytes = memoryPoolCapacityInMB * 1024L * 1024L;
    }

    /**
     * Returns the actual space consumed by index summaries of non-compacting sstables in MB.
     * @return space currently used in MB
     */
    public double getMemoryPoolSizeInMB()
    {
        long total = 0;
        for (SSTableReader sstable : getAllNoncompactingSSTables())
            total += sstable.getIndexSummaryOffHeapSize();
        return total / 1024.0 / 1024.0;
    }

    private List<SSTableReader> getAllNoncompactingSSTables()
    {
        List<SSTableReader> sstables = new ArrayList<>();
        for (Keyspace ks : Keyspace.all())
            for (ColumnFamilyStore cfStore: ks.getColumnFamilyStores())
                sstables.addAll(cfStore.getDataTracker().getUncompactingSSTables());
        return sstables;
    }

    public void redistributeSummaries() throws IOException
    {
        redistributeSummaries(getAllNoncompactingSSTables(), this.memoryPoolBytes);
    }

    /**
     * Attempts to fairly distribute a fixed pool of memory for index summaries across a set of SSTables based on
     * their recent read rates.
     * @param sstables a list of sstables to share the memory pool across
     * @param memoryPoolBytes a size (in bytes) that the total index summary space usage should stay close to or
     *                           under, if possible
     */
    @VisibleForTesting
    public static void redistributeSummaries(List<SSTableReader> sstables, long memoryPoolBytes) throws IOException
    {
        logger.debug("Beginning redistribution of index summaries for {} sstables with memory pool size {} MB",
                     sstables.size(), memoryPoolBytes / 1024L / 1024L);

        double totalReadsPerSec = 0.0;
        for (SSTableReader sstable : sstables)
        {
            if (sstable.readMeter != null)
            {
                totalReadsPerSec += sstable.readMeter.fifteenMinuteRate();
            }
        }
        logger.trace("Total reads/sec across all sstables in index summary resize process: {}", totalReadsPerSec);

        // copy and sort by read rates (ascending)
        List<SSTableReader> sstablesByHotness = new ArrayList<>(sstables);
        Collections.sort(sstablesByHotness, new Comparator<SSTableReader>()
        {
            public int compare(SSTableReader o1, SSTableReader o2)
            {
                if (o1.readMeter == null && o2.readMeter == null)
                    return 0;
                else if (o1.readMeter == null)
                    return -1;
                else if (o2.readMeter == null)
                    return 1;
                else
                    return Double.compare(o1.readMeter.fifteenMinuteRate(), o2.readMeter.fifteenMinuteRate());
            }
        });

        List<Pair<SSTableReader, IndexSummary>> sstablesWithSummaries = new ArrayList<>(sstablesByHotness.size());
        for (SSTableReader sstable : sstablesByHotness)
            sstablesWithSummaries.add(Pair.create(sstable, sstable.getReferencedIndexSummary()));
        try
        {
            adjustSamplingLevels(sstablesWithSummaries, totalReadsPerSec, memoryPoolBytes);
        }
        finally
        {
            for (Pair<SSTableReader, IndexSummary> pair : sstablesWithSummaries)
                pair.right.unreference();
        }

        long total = 0;
        for (SSTableReader sstable : sstablesByHotness)
            total += sstable.getIndexSummaryOffHeapSize();
        logger.debug("Completed resizing of index summaries; current approximate memory used: {} MB",
                     total / 1024.0 / 1024.0);
    }

    private static void adjustSamplingLevels(List<Pair<SSTableReader, IndexSummary>> sstablesWithSummaries,
                                             double totalReadsPerSec, long memoryPoolCapacity) throws IOException
    {

        // list of (SSTR, (newSpaceUsed, newSamplingLevel)) pairs
        List<ResampleEntry> toDownsample = new ArrayList<>(sstablesWithSummaries.size() / 4);

        // list of (SSTR, newSamplingLevel) pairs
        List<ResampleEntry> toUpsample = new ArrayList<>(sstablesWithSummaries.size() / 4);

        // Going from the coldest to the hottest sstables, try to give each sstable an amount of space proportional
        // to the number of total reads/sec it handles.
        long remainingSpace = memoryPoolCapacity;
        for (Pair<SSTableReader, IndexSummary> pair : sstablesWithSummaries)
        {
            SSTableReader sstable = pair.left;
            IndexSummary summary = pair.right;

            double readsPerSec = sstable.readMeter == null ? 0.0 : sstable.readMeter.fifteenMinuteRate();
            long idealSpace = Math.round(remainingSpace * (readsPerSec / totalReadsPerSec));

            // figure out how many entries our idealSpace would buy us, and pick a new sampling level based on that
            double avgEntrySize = summary.getOffHeapSize() / (double) summary.size();
            long targetNumEntries = Math.round(idealSpace / avgEntrySize);
            int newSamplingLevel = IndexSummaryBuilder.calculateSamplingLevel(summary, targetNumEntries);

            logger.trace("{} has {} reads/sec; ideal space for index summary: {} bytes; target number of retained entries: {}",
                         sstable.getFilename(), readsPerSec, idealSpace, targetNumEntries);

            if (targetNumEntries >= summary.size() * UPSAMPLE_THRESHOLD && newSamplingLevel > summary.getSamplingLevel())
            {
                long spaceUsed = (long) Math.ceil(avgEntrySize * IndexSummaryBuilder.entriesAtSamplingLevel(summary, newSamplingLevel));
                toUpsample.add(new ResampleEntry(sstable, summary, spaceUsed, newSamplingLevel));
                remainingSpace -= avgEntrySize * IndexSummaryBuilder.entriesAtSamplingLevel(summary, newSamplingLevel);
            }
            else if (targetNumEntries < summary.size() * DOWNSAMPLE_THESHOLD && newSamplingLevel < summary.getSamplingLevel())
            {
                long spaceUsed = (long) Math.ceil(avgEntrySize * IndexSummaryBuilder.entriesAtSamplingLevel(summary, newSamplingLevel));
                toDownsample.add(new ResampleEntry(sstable, summary, spaceUsed, newSamplingLevel));
                remainingSpace -= spaceUsed;
            }
            else
            {
                // keep the same sampling level
                logger.trace("SSTable is within thresholds of ideal sampling");
                remainingSpace -= summary.getOffHeapSize();
            }
            totalReadsPerSec -= readsPerSec;
        }

        toDownsample = distributeRemainingSpace(toDownsample, remainingSpace);
        for (ResampleEntry entry : toDownsample)
        {
            logger.debug("Downsampling index summary for {} from {}/{} to {}/{} of the original number of entries",
                         entry.sstable, entry.summary.getSamplingLevel(), Downsampling.BASE_SAMPLING_LEVEL,
                         entry.newSamplingLevel, Downsampling.BASE_SAMPLING_LEVEL);
            IndexSummary newSummary = IndexSummaryBuilder.downsample(entry.summary, entry.newSamplingLevel, entry.sstable.partitioner);
            entry.sstable.setIndexSummary(newSummary);
        }

        for (ResampleEntry entry : toUpsample)
        {
            logger.debug("Upsampling index summary for {} from {}/{} to {}/{} of the original number of entries",
                         entry.sstable, entry.summary.getSamplingLevel(), Downsampling.BASE_SAMPLING_LEVEL,
                         entry.newSamplingLevel, Downsampling.BASE_SAMPLING_LEVEL);
            try
            {
                entry.sstable.rebuildSummary(entry.newSamplingLevel);
            }
            catch (IOException ioe)
            {
                logger.error("Failed to rebuild index summary: ", ioe);
            }
        }
    }

    @VisibleForTesting
    static List<ResampleEntry> distributeRemainingSpace(List<ResampleEntry> toDownsample, long remainingSpace)
    {
        // sort by read rate (descending)
        Collections.sort(toDownsample, new Comparator<ResampleEntry>()
        {
            public int compare(ResampleEntry o1, ResampleEntry o2)
            {
                RestorableMeter a = o1.sstable.readMeter;
                RestorableMeter b = o2.sstable.readMeter;
                if (a == null && b == null)
                    return 0;
                else if (a == null)
                    return 1;
                else if (b == null)
                    return -1;
                else
                    return -1 * Double.compare(a.fifteenMinuteRate(), b.fifteenMinuteRate());
            }
        });

        while (remainingSpace > 0 && !toDownsample.isEmpty())
        {
            boolean didAdjust = false;
            List<ResampleEntry> newToDownsample = new ArrayList<>(toDownsample.size());
            for (ResampleEntry entry : toDownsample)
            {
                int entriesAtNextLevel = IndexSummaryBuilder.entriesAtSamplingLevel(entry.summary, entry.newSamplingLevel + 1);
                double avgEntrySize = entry.summary.getOffHeapSize() / (double) entry.summary.size();
                long spaceAtNextLevel = (long) Math.ceil(avgEntrySize * entriesAtNextLevel);
                long extraSpaceRequired = (spaceAtNextLevel - entry.newSpaceUsed);
                // see if we have enough leftover space to increase the sampling level
                if (extraSpaceRequired <= remainingSpace)
                {
                    didAdjust = true;
                    remainingSpace -= extraSpaceRequired;
                    // if increasing the level would put us back to the original level, just leave this sstable out
                    // of the set to downsample
                    if (entry.newSamplingLevel + 1 < entry.summary.getSamplingLevel())
                        newToDownsample.add(new ResampleEntry(entry.sstable, entry.summary, spaceAtNextLevel, entry.newSamplingLevel + 1));
                }
                else
                {
                    newToDownsample.add(entry);
                }
            }
            toDownsample = newToDownsample;
            if (!didAdjust)
                break;
        }
        return toDownsample;
    }

    private static class ResampleEntry
    {
        public final SSTableReader sstable;
        public final IndexSummary summary;
        public final long newSpaceUsed;
        public final int newSamplingLevel;

        public ResampleEntry(SSTableReader sstable, IndexSummary summary, long newSpaceUsed, int newSamplingLevel)
        {
            this.sstable = sstable;
            this.summary = summary;
            this.newSpaceUsed = newSpaceUsed;
            this.newSamplingLevel = newSamplingLevel;
        }
    }
}