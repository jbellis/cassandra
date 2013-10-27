package org.apache.cassandra.stress.util;

import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CountDownLatch;

// a timer - this timer must be used by a single thread, and co-ordinates with other timers by
public final class Timer
{

    private static final int SAMPLE_SIZE_SHIFT = 10;
    private static final int SAMPLE_SIZE_MASK = (1 << SAMPLE_SIZE_SHIFT) - 1;

    private final Random rnd = new Random();

    // in progress snap start
    private long sampleStartNanos;

    // each entry is present with probability 1/p(opCount) or 1/(p(opCount)-1)
    private final long[] sample = new long[1 << SAMPLE_SIZE_SHIFT];
    private int opCount;

    // the four longest latencies, and the times at which they started
    // in (length, start) pairs, in increasing length order
    private static final int MAX_SAVE_COUNT = 4;
    private final long[] max = new long[MAX_SAVE_COUNT * 2];

    // aggregate info
    private int keyCount;
    private long total;
    private long upToDateAsOf;
    private long lastSnap = System.currentTimeMillis();

    // communication with summary/logging thread
    private volatile CountDownLatch reportRequest;
    volatile TimingInterval report;
    private volatile TimingInterval finalReport;

    public void start(){
        // decide if we're logging this event
        sampleStartNanos = System.nanoTime();
    }

    private static int p(int index)
    {
        return 1 + (index >>> SAMPLE_SIZE_SHIFT);
    }

    private void updateMax(long length, long start)
    {
        // quick terminate to help branch predictor
        if (length < max[0])
            return;
        int i;
        // find position to insert
        for (i = 0 ; i < MAX_SAVE_COUNT ; i += 2)
        {
            if (max[i] == 0)
            {
                // empty insert, so just place and return
                max[i] = length;
                max[i + 1] = start;
                return;
            }
            // found item larger, so
            if (max[i] > length)
                break;
        }
        if (max[MAX_SAVE_COUNT - 1] == 0)
        {
            // not full, so shuffle up
            for (int j = MAX_SAVE_COUNT ; j > i + 2 ; j -= 2)
            {
                max[j - 2] = max[j - 4];
                max[j - 1] = max[j - 3];
            }
        }
        else
        {
            // full, so shuffle down (dropping smallest)

        }
        max[i] = length;
        max[i + 1] = start;
    }

    public void stop(int keys)
    {
        maybeReport();
        long time = System.nanoTime() - sampleStartNanos;
        if (rnd.nextInt(p(opCount)) == 0)
            sample[index(opCount)] = time;
        if (time > max)
            max = time;
        total += time;
        opCount += 1;
        keyCount += keys;
        upToDateAsOf = System.currentTimeMillis();
    }

    private static int index(int count)
    {
        return count & SAMPLE_SIZE_MASK;
    }

    private TimingInterval buildReport()
    {
        final List<SampleOfLongs> sampleLatencies = Arrays.asList
                (       new SampleOfLongs(Arrays.copyOf(sample, index(opCount)), p(opCount)),
                        new SampleOfLongs(Arrays.copyOfRange(sample, index(opCount), Math.min(opCount, sample.length)), p(opCount) - 1)
                );
        final TimingInterval report = new TimingInterval(lastSnap, upToDateAsOf, max, keyCount, total, opCount,
                SampleOfLongs.merge(rnd, sampleLatencies, Integer.MAX_VALUE));
        // reset counters
        opCount = 0;
        keyCount = 0;
        total = 0;
        max = 0;
        lastSnap = upToDateAsOf;
        return report;
    }

    // checks to see if a report has been requested, and if so produces the report, signals and clears the request
    private void maybeReport()
    {
        if (reportRequest != null)
        {
            synchronized (this)
            {
                report = buildReport();
                reportRequest.countDown();
                reportRequest = null;
            }
        }
    }

    // checks to see if the timer is dead; if not requests a report, and otherwise fulfills the request itself
    synchronized void requestReport(CountDownLatch signal)
    {
        if (finalReport != null)
        {
            report = finalReport;
            finalReport = new TimingInterval(0);
            signal.countDown();
        }
        else
            reportRequest = signal;
    }

    // closes the timer; if a request is outstanding, it furnishes the request, otherwise it populates finalReport
    public synchronized void close()
    {
        if (reportRequest == null)
            finalReport = buildReport();
        else
        {
            finalReport = new TimingInterval(0);
            report = buildReport();
            reportRequest.countDown();
            reportRequest = null;
        }
    }

}

