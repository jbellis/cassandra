package org.apache.cassandra.stress.generatedata;

public class DataGenHexInSeqBatch extends DataGenHex
{

    final DataGenHex delegate;
    final int batchSize;
    final long maxKey;

    private int batchIndex;
    private long batchKey;

    // object must be published safely if passed between threads, due to batchIndex not being volatile. various
    // hacks possible, but not ideal. don't want to use volatile as object intended for single threaded use.
    public DataGenHexInSeqBatch(int batchSize, long maxKey, DataGenHex delegate)
    {
        this.batchIndex = batchSize;
        this.batchSize = batchSize;
        this.maxKey = maxKey;
        this.delegate = delegate;
    }

    @Override
    long nextInt(long operationIndex)
    {
        if (batchIndex >= batchSize)
        {
            batchKey = delegate.nextInt(operationIndex);
            batchIndex = 0;
        }
        long r = batchKey + batchIndex++;
        if (r > maxKey)
        {
            batchKey = delegate.nextInt(operationIndex);
            batchIndex = 1;
            r = batchKey;
        }
        return r;
    }

    @Override
    public boolean deterministic()
    {
        return false;
    }

}
