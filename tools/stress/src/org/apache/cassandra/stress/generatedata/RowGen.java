package org.apache.cassandra.stress.generatedata;

import java.nio.ByteBuffer;
import java.util.List;

/**
 * Generates a row of data, by constructing one byte buffers per column according to some algorithm
 * and delegating the work of populating the values of those byte buffers to the provided data generator
 */
public abstract class RowGen
{

    final DataGen dataGen;
    final int maxUniqueColumns;
    protected RowGen(DataGen dataGenerator, int maxUniqueColumns)
    {
        this.dataGen = dataGenerator;
        this.maxUniqueColumns = maxUniqueColumns;
    }

    public List<ByteBuffer> generate(long operationIndex)
    {
        List<ByteBuffer> columns = getColumns(operationIndex);

        // only fill columns up to the max unique count
        List<ByteBuffer> fill;
        if (maxUniqueColumns < columns.size())
            fill = columns.subList(0, maxUniqueColumns);
        else
            fill = columns;

        dataGen.generate(fill, operationIndex);

        // if there were more columns than we permit unique then duplicate them
        if (fill.size() != columns.size())
            for (int i = fill.size() ; i < columns.size() ; i++)
                columns.set(i, fill.get(i % fill.size()).duplicate());

        return columns;
    }

    // these byte[] may be re-used
    abstract List<ByteBuffer> getColumns(long operationIndex);

    abstract public boolean deterministic();

}
