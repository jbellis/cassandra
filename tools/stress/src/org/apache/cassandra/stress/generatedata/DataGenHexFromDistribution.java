package org.apache.cassandra.stress.generatedata;

import org.apache.commons.math3.distribution.*;

public class DataGenHexFromDistribution extends DataGenHex
{

    final AbstractRealDistribution distribution;
    final long minKey;
    final long maxKey;

    public DataGenHexFromDistribution(AbstractRealDistribution distribution, long minKey, long maxKey)
    {
        this.distribution = distribution;
        this.minKey = minKey;
        this.maxKey = maxKey;
    }

    @Override
    public boolean deterministic()
    {
        return false;
    }

    @Override
    long nextInt(long operationIndex)
    {
        while (true)
        {
            final long v = (long) distribution.sample();
            if (v >= minKey && v <= maxKey)
                return v;
        }
    }

    public static DataGenHex buildGaussian(long minKey, long maxKey, double stdevsToLimit)
    {
        double midRange = (maxKey + minKey) / 2d;
        double halfRange = (maxKey - minKey) / 2d;
        return new DataGenHexFromDistribution(new NormalDistribution(midRange, halfRange / stdevsToLimit), minKey, maxKey);
    }

    public static DataGenHex buildUniform(long minKey, long maxKey, double stdevsToLimit)
    {
        return new DataGenHexFromDistribution(new UniformRealDistribution(minKey, maxKey), minKey, maxKey);
    }

}
