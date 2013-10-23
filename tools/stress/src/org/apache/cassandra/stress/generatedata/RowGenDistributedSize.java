package org.apache.cassandra.stress.generatedata;

import org.apache.commons.math3.distribution.*;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

public class RowGenDistributedSize extends RowGen
{

    // TODO : fix caching so that very large rows still get column sizes distributed as per the provided distribution
    // TODO IMMEDIATELY : fix caching so

    static final int MAX_BUFFER_CACHE_SIZE = 1024;
    static final int MAX_COLUMN_COUNT = 1024;

    final Distribution countDistribution;
    final Distribution sizeDistribution;

    // caches the possible sizes of bytebuffer
    final ByteBuffer[] columns;
    // temporary array for guaranteeing no simultaneous use of a buffer
    final boolean[] used;
    // array re-used for returning columns
    final ByteBuffer[] ret;

    public RowGenDistributedSize(DataGen dataGenerator, Distribution countDistribution, Distribution sizeDistribution)
    {
        super(dataGenerator);
        this.countDistribution = countDistribution;
        this.sizeDistribution = sizeDistribution;
        // reuse anything up to 1K
        columns = new ByteBuffer[Math.min(MAX_BUFFER_CACHE_SIZE, (int) sizeDistribution.maxValue())];
        for (int i = 0 ; i < columns.length ; i++)
            columns[i] = ByteBuffer.wrap(new byte[i]);
        used = new boolean[columns.length];
        ret = new ByteBuffer[Math.min(MAX_COLUMN_COUNT, (int) sizeDistribution.maxValue())];
    }

    @Override
    List<ByteBuffer> getColumns(long operationIndex)
    {
        int i = 0;
        while (i < ret.length)
        {
            int columnSize = (int) sizeDistribution.next();
            while (columnSize < used.length && used[columnSize])
                columnSize++;
            if (columnSize < used.length)
            {
                used[columnSize] = true;
                ret[i] = columns[columnSize];
            }
            else
                ret[i] = ByteBuffer.allocate(columnSize);
            i++;
        }
        for ( i = 0 ; i < ret.length ; i++ )
            if (ret[i].capacity() < used.length)
                used[ret[i].capacity()] = false;
        return Arrays.asList(ret);
    }

    @Override
    public boolean deterministic()
    {
        return false;
    }

}
