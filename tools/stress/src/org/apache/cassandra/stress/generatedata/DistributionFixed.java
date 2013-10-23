package org.apache.cassandra.stress.generatedata;

import org.apache.commons.math3.distribution.*;

public class DistributionFixed implements Distribution
{

    final long key;

    public DistributionFixed(long key)
    {
        this.key = key;
    }

    @Override
    public long next()
    {
        return key;
    }

    @Override
    public long maxValue()
    {
        return key;
    }

}