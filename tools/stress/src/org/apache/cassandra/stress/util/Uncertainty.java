package org.apache.cassandra.stress.util;

import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;

// TODO: do not assume normal distribution of measurements.
public class Uncertainty
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

    public void update(double value)
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

    public void await(double targetUncertainty, int measurements) throws InterruptedException
    {
        final WaitForTargetUncertainty wait = new WaitForTargetUncertainty(targetUncertainty, measurements);
        waiting.add(wait);
        wait.await();
    }

    public double getUncertainty()
    {
        return uncertainty;
    }

    public void wakeAll()
    {
        for (WaitForTargetUncertainty waiting : this.waiting)
        {
            waiting.latch.countDown();
            this.waiting.remove(waiting);
        }
    }

}
