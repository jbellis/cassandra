package org.apache.cassandra.stress.generatedata;

public interface Distribution
{

    long next();
    long maxValue();
    long minValue();

}