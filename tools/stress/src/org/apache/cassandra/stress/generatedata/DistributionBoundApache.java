package org.apache.cassandra.stress.generatedata;

import org.apache.commons.math3.distribution.*;

public class DistributionBoundApache implements Distribution
{

    final AbstractRealDistribution delegate;
    final long minKey;
    final long maxKey;

    public DistributionBoundApache(AbstractRealDistribution delegate, long minKey, long maxKey)
    {
        this.delegate = delegate;
        this.minKey = minKey;
        this.maxKey = maxKey;
    }

    @Override
    public long next()
    {
        while (true)
        {
            long r = (long) delegate.sample();
            if ((r >= minKey) & (r <= maxKey))
                return r;
        }
    }

    @Override
    public long maxValue()
    {
        return Math.min(maxKey, (long) delegate.inverseCumulativeProbability(1d));
    }

}