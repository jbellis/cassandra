package org.apache.cassandra.stress.generatedata;

import org.apache.commons.math3.distribution.*;

public class DistributionOffsetApache implements Distribution
{

    final AbstractRealDistribution delegate;
    final long minKey;
    final long maxKey;

    public DistributionOffsetApache(AbstractRealDistribution delegate, long minKey, long maxKey)
    {
        this.delegate = delegate;
        this.minKey = minKey;
        this.maxKey = maxKey;
    }

    @Override
    public long next()
    {
        long delta = maxKey - minKey;
        while (true)
        {
            long r = (long) delegate.sample();
            if (r < delta)
                return minKey + r;
        }
    }

    @Override
    public long maxValue()
    {
        return Math.min(maxKey, minKey + (long) delegate.inverseCumulativeProbability(1d));
    }

    @Override
    public long minValue()
    {
        return minKey + (long) delegate.inverseCumulativeProbability(0d);
    }

}