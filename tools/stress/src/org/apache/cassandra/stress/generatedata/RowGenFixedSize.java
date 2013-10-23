package org.apache.cassandra.stress.generatedata;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;

public class RowGenFixedSize extends RowGen
{

    final List<ByteBuffer> columns;
    public RowGenFixedSize(DataGen dataGenerator, int columnCount, int uniqueColumnCount, int columnSize)
    {
        super(dataGenerator, uniqueColumnCount);
        final ByteBuffer[] buffers = new ByteBuffer[columnCount];
        for (int i = 0 ; i < buffers.length ; i++)
            buffers[i] = ByteBuffer.wrap(new byte[columnSize]);
        this.columns = Arrays.asList(buffers);
    }

    @Override
    List<ByteBuffer> getColumns(long operationIndex)
    {
        return columns;
    }

    @Override
    public boolean deterministic()
    {
        return dataGen.deterministic();
    }

}
