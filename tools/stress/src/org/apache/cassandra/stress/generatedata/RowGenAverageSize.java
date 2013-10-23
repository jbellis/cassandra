package org.apache.cassandra.stress.generatedata;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

public class RowGenAverageSize extends RowGen
{

    final Random rnd = new Random();

    // caches the possible sizes of bytebuffer
    final ByteBuffer[] columns;
    // temporary array for guaranteeing no simultaneous use of a buffer
    final boolean[] used;
    // array re-used for returning columns
    final ByteBuffer[] ret;

    public RowGenAverageSize(DataGen dataGenerator, int maxUniqueColumns, int columnCount, int averageColumnSize)
    {
        super(dataGenerator, maxUniqueColumns);
        columns = new ByteBuffer[averageColumnSize << 1];
        for (int i = 0 ; i < columns.length ; i++)
            columns[i] = ByteBuffer.wrap(new byte[i]);
        used = new boolean[columns.length];
        ret = new ByteBuffer[columnCount];
    }

    @Override
    List<ByteBuffer> getColumns(long operationIndex)
    {
        int i = 0;
        while (i < ret.length)
        {
            final int usei = rnd.nextInt(columns.length);
            if (used[usei])
                continue;
            used[usei] = true;
            ret[i] = columns[usei];
            i++;
        }
        for ( i = 0 ; i < ret.length ; i++ )
            used[ret[i].capacity()] = false;
        return Arrays.asList(columns);
    }

    @Override
    public boolean deterministic()
    {
        return false;
    }

}
