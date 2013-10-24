package org.apache.cassandra.stress;

import org.apache.cassandra.concurrent.NamedThreadFactory;
import org.apache.commons.lang3.time.*;

import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class StressMetrics
{

    // TODO : maybe store a subsample of each interval's latency samples, so can easily calculate latencies over arbitrary intervals

    private final PrintStream output;
    private final ExecutorService exec = Executors.newFixedThreadPool(1, new NamedThreadFactory("StressMetrics"));
    private volatile boolean stop = false;
    private final CopyOnWriteArrayList<Timer> timers = new CopyOnWriteArrayList<>();
    private final Random rnd = new Random();
    private volatile TimerInterval fullHistory;
    private final Uncertainty opRateUncertainty = new Uncertainty();

    public static final String HEADFORMAT = "%-10s,%8s,%8s,%5s,%5s,%6s,%8s,%8s,%8s,%7s,%7s";
    public static final String ROWFORMAT =  "%-10d,%8.0f,%8.0f,%5.1f,%5.1f,%6.1f,%8.1f,%8.1f,%8.1f,%7.1f,%7.3f";

    private static void printHeader(String prefix, PrintStream output)
    {
        output.println(prefix + String.format(HEADFORMAT, "ops","command/s","key/s","mean","med",".95",".99",".999","max","time","err"));
    }

    private static void printRow(String prefix, TimerInterval interval, TimerInterval total, Uncertainty opRateUncertainty, PrintStream output)
    {
        output.println(prefix + String.format(ROWFORMAT,
                total.operationCount,
                interval.opRate(),
                interval.keyRate(),
                interval.meanLatency(),
                interval.medianLatency(),
                interval.rankLatency(0.95f),
                interval.rankLatency(0.99f),
                interval.rankLatency(0.999f),
                interval.maxLatency(),
                total.runTime() / 1000f,
                opRateUncertainty.uncertainty));
    }

    public StressMetrics(PrintStream output)
    {
        this.output = output;
        printHeader("", output);
    }

    public TimerInterval getFullHistory()
    {
        return fullHistory;
    }

    public void meterWithLogInterval(final int intervalMillis)
    {

        exec.execute(new Runnable()
        {
            @Override
            public void run()
            {
                fullHistory = new TimerInterval(System.currentTimeMillis());
                while (!stop)
                {
                    try
                    {
                        long sleep = fullHistory.end + intervalMillis - System.currentTimeMillis();
                        if (sleep > 0)
                            Thread.sleep(sleep);
                        update();
                    } catch (InterruptedException e)
                    {
                    }
                }
                try
                {
                    update();
                } catch (InterruptedException e)
                {
                }
            }
        });
    }

    public static final void summarise(List<String> ids, List<StressMetrics> summarise, PrintStream out)
    {
        int idLen = 0;
        for (String id : ids)
            idLen = Math.max(id.length(), idLen);
        String formatstr = "%" + idLen + "s, ";
        printHeader(String.format(formatstr, "id"), out);
        for (int i = 0 ; i < ids.size() ; i++)
            printRow(String.format(formatstr, ids.get(i)),
                    summarise.get(i).fullHistory,
                    summarise.get(i).fullHistory,
                    summarise.get(i).opRateUncertainty,
                    out
            );
    }

    private void update() throws InterruptedException
    {
        final TimerInterval interval = snapInterval(rnd);
        fullHistory = TimerInterval.merge(rnd, Arrays.asList(interval, fullHistory), 50000, fullHistory.start);
        printRow("", interval, fullHistory, opRateUncertainty, output);
        opRateUncertainty.update(interval.opRate());
    }

    // TODO: do not assume normal distribution of measurements.
    private static class Uncertainty
    {

        private int measurements;
        private double sumsquares;
        private double sum;
        private double stdev;
        private double mean;
        private double uncertainty;

        private CopyOnWriteArrayList<WaitForTargetUncertainty> waiting = new CopyOnWriteArrayList<>();

        private static final class WaitForTargetUncertainty
        {
            final double targetUncertainty;
            final int measurements;
            final CountDownLatch latch = new CountDownLatch(1);

            private WaitForTargetUncertainty(double targetUncertainty, int measurements)
            {
                this.targetUncertainty = targetUncertainty;
                this.measurements = measurements;
            }

            void await() throws InterruptedException
            {
                latch.await();
            }

        }

        private void update(double value)
        {
            measurements++;
            sumsquares += value * value;
            sum += value;
            mean = sum / measurements;
            stdev = Math.sqrt((sumsquares / measurements) - (mean * mean));
            uncertainty = (stdev / Math.sqrt(measurements)) / mean;

            for (WaitForTargetUncertainty waiter : waiting)
            {
                if (uncertainty < waiter.targetUncertainty && measurements >= waiter.measurements)
                {
                    waiter.latch.countDown();
                    // can safely remove as working over snapshot with COWArrayList
                    waiting.remove(waiter);
                }
            }
        }

        private void await(double targetUncertainty, int measurements) throws InterruptedException
        {
            final WaitForTargetUncertainty wait = new WaitForTargetUncertainty(targetUncertainty, measurements);
            waiting.add(wait);
            wait.await();
        }

    }

    public void runUntilConverges(double targetUncertainty, int minMeasurements) throws InterruptedException
    {
        opRateUncertainty.await(targetUncertainty, minMeasurements);
    }

    public void stop()
    {
        stop = true;
        exec.shutdownNow();
        try
        {
            exec.awaitTermination(Long.MAX_VALUE, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e)
        {
            throw new IllegalStateException();
        }
    }

    public void summarise()
    {
        output.println("\n");
        output.println("Results:");
        output.println(String.format("command rate                   : %.0f", fullHistory.opRate()));
        output.println(String.format("key rate                  : %.0f", fullHistory.keyRate()));
        output.println(String.format("latency mean              : %.1f", fullHistory.meanLatency()));
        output.println(String.format("latency median            : %.1f", fullHistory.medianLatency()));
        output.println(String.format("latency 95th percentile   : %.1f", fullHistory.rankLatency(.95f)));
        output.println(String.format("latency 99th percentile   : %.1f", fullHistory.rankLatency(0.99f)));
        output.println(String.format("latency 99.9th percentile : %.1f", fullHistory.rankLatency(0.999f)));
        output.println(String.format("latency max               : %.1f", fullHistory.maxLatency()));
        output.println("Total operation time      : " + DurationFormatUtils.formatDuration(
                fullHistory.runTime(), "HH:mm:ss", true));
    }

    private TimerInterval snapInterval(Random rnd) throws InterruptedException
    {
        final Timer[] timers = this.timers.toArray(new Timer[0]);
        final CountDownLatch ready = new CountDownLatch(timers.length);
        for (int i = 0 ; i < timers.length ; i++)
        {
            final Timer timer = timers[i];
            timer.requestReport(ready);
        }

        // TODO fail gracefully after timeout if a thread is stuck
        ready.await();

        // reports have been filled in by timer threadCount, so merge
        List<TimerInterval> intervals = new ArrayList<>();
        for (Timer timer : timers)
            intervals.add(timer.report);

        return TimerInterval.merge(rnd, intervals, Integer.MAX_VALUE, fullHistory.end);
    }

    public static final class TimerInterval
    {
        // millis
        public final long start;
        public final long end;

        // nanos
        public final long maxLatency;
        public final long totalLatency;

        // discrete
        public final long keyCount;
        public final long operationCount;

        final TimerLatencies[] sampleLatencies;

        private TimerInterval(long time)
        {
            start = end = time;
            maxLatency = totalLatency = 0;
            keyCount = operationCount = 0;
            sampleLatencies = new TimerLatencies[0];
        }
        private TimerInterval(long start, long end, long maxLatency, long keyCount, long totalLatency, long operationCount, TimerLatencies[] sampleLatencies)
        {
            this.start = start;
            this.end = Math.max(end, start);
            this.maxLatency = maxLatency;
            this.keyCount = keyCount;
            this.totalLatency = totalLatency;
            this.operationCount = operationCount;
            this.sampleLatencies = sampleLatencies;
        }

        private static TimerInterval merge(Random rnd, List<TimerInterval> intervals, int maxSamples, long start)
        {
            int operationCount = 0, keyCount = 0;
            long maxLatency = 0, totalLatency = 0;
            List<TimerLatencies> latencies = new ArrayList<>();
            long end = 0;
            for (TimerInterval interval : intervals)
            {
                end = Math.max(end, interval.end);
                operationCount += interval.operationCount;
                maxLatency = Math.max(interval.maxLatency, maxLatency);
                totalLatency += interval.totalLatency;
                keyCount += interval.keyCount;
                latencies.addAll(Arrays.asList(interval.sampleLatencies));
            }

            return new TimerInterval(start, end, maxLatency, keyCount, totalLatency, operationCount,
                    new TimerLatencies[] { TimerLatencies.merge(rnd, latencies, maxSamples) });

        }

        public double opRate()
        {
            return operationCount / ((end - start) * 0.001d);
        }

        public double keyRate()
        {
            return keyCount / ((end - start) * 0.001d);
        }

        public double meanLatency()
        {
            return (totalLatency / (double) operationCount) * 0.000001d;
        }

        public double maxLatency()
        {
            return maxLatency * 0.000001d;
        }

        public double medianLatency()
        {
            assert sampleLatencies.length == 1;
            if (sampleLatencies.length == 0)
                return 0;
            final long[] sample = sampleLatencies[0].sample;
            if (sample.length == 0)
                return 0;
            return sample[sample.length >> 1] * 0.000001d;
        }

        // 0 < rank < 1
        public double rankLatency(float rank)
        {
            assert sampleLatencies.length == 1;
            if (sampleLatencies.length == 0)
                return 0;
            final long[] sample = sampleLatencies[0].sample;
            if (sample.length == 0)
                return 0;
            int index = (int)(rank * sample.length);
            if (index >= sample.length)
                index = sample.length - 1;
            return sample[index] * 0.000001d;
        }

        public long runTime()
        {
            return end - start;
        }
    }

    private static final class TimerLatencies
    {
        // nanos
        final long[] sample;

        // probability with which each sample was selected
        final double p;

        private TimerLatencies(long[] sample, int p)
        {
            this.sample = sample;
            this.p = 1 / (float) p;
        }

        private TimerLatencies(long[] sample, double p)
        {
            this.sample = sample;
            this.p = p;
        }

        static TimerLatencies merge(Random rnd, List<TimerLatencies> merge, int maxSamples)
        {
            int maxLength = 0;
            double targetp = 1;
            for (TimerLatencies latencies : merge)
            {
                maxLength += latencies.sample.length;
                targetp = Math.min(targetp, latencies.p);
            }
            long[] sample = new long[maxLength];
            int count = 0;
            for (TimerLatencies latencies : merge)
            {
                long[] in = latencies.sample;
                double p = targetp / latencies.p;
                for (int i = 0 ; i < in.length ; i++)
                    if (rnd.nextDouble() < p)
                        sample[count++] = in[i];
            }
            if (count > maxSamples)
            {
                targetp = subsample(rnd, maxSamples, sample, count, targetp);
                count = maxSamples;
            }
            sample = Arrays.copyOf(sample, count);
            Arrays.sort(sample);
            return new TimerLatencies(sample, targetp);
        }

        private TimerLatencies subsample(Random rnd, int maxSamples)
        {
            if (maxSamples > sample.length)
                return this;

            long[] sample = this.sample.clone();
            double p = subsample(rnd, maxSamples, sample, sample.length, this.p);
            sample = Arrays.copyOf(sample, maxSamples);
            return new TimerLatencies(sample, p);
        }

        private static double subsample(Random rnd, int maxSamples, long[] sample, int count, double p)
        {
            // want exactly maxSamples, so select random indexes up to maxSamples
            for (int i = 0 ; i < maxSamples ; i++)
            {
                int take = i + rnd.nextInt(count - i);
                long tmp = sample[i];
                sample[i] = sample[take];
                sample[take] = tmp;
            }

            // calculate new p; have selected with probability maxSamples / count
            // so multiply p by this probability
            p *= maxSamples / (double) sample.length;
            return p;
        }

    }

    public Timer newTimer()
    {
        final Timer timer = new Timer();
        timers.add(timer);
        return timer;
    }

    public static final class Timer
    {

        private static final int SAMPLE_SIZE_SHIFT = 10;
        private static final int SAMPLE_SIZE = 1 << SAMPLE_SIZE_SHIFT;
        private static final int SAMPLE_SIZE_MASK = (1 << SAMPLE_SIZE_SHIFT) - 1;

        private final Random rnd = new Random();

        // in progress snap start
        private long sampleStartNanos;

        // each entry is present with probability 1/p(opCount) or 1/(p(opCount)-1)
        private long[] sample = new long[1 << SAMPLE_SIZE_SHIFT];
        private int opCount;

        // aggregate info
        private int keyCount;
        private long total;
        private long max;
        private long upToDateAsOf;
        private long lastSnap = System.currentTimeMillis();

        // communication with summary/logging thread
        private volatile CountDownLatch reportRequest;
        private volatile TimerInterval report;
        private volatile TimerInterval finalReport;

        public void start(){
            // decide if we're logging this event
            sampleStartNanos = System.nanoTime();
        }

        private static int p(int index)
        {
            return 1 + (index >>> SAMPLE_SIZE_SHIFT);
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

        private TimerInterval buildReport()
        {
            final TimerLatencies[] sampleLatencies = new TimerLatencies[]
                    {       new TimerLatencies(Arrays.copyOf(sample, index(opCount)), p(opCount)),
                            new TimerLatencies(Arrays.copyOfRange(sample, index(opCount), Math.min(opCount, sample.length)), p(opCount) - 1)
                    };
            final TimerInterval report = new TimerInterval(lastSnap, upToDateAsOf, max, keyCount, total, opCount, sampleLatencies);
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
        private synchronized void requestReport(CountDownLatch signal)
        {
            if (finalReport != null)
            {
                report = finalReport;
                finalReport = new TimerInterval(0);
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
                finalReport = new TimerInterval(0);
                report = buildReport();
                reportRequest.countDown();
                reportRequest = null;
            }
        }

    }

}
