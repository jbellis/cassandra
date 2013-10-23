package org.apache.cassandra.stress.settings;

import java.util.Arrays;
import java.util.List;

public enum OpType
{

    INSERT, READ, MIXED(INSERT, READ), RANGE_SLICE, INDEXED_RANGE_SLICE, READMULTI, COUNTERWRITE, COUNTERREAD;

    OpType()
    {
        warmups = new OpType[]{this};
    }
    OpType(OpType ... warmups)
    {
        this.warmups = warmups;
    }
    private OpType[] warmups;

    public List<OpType> getWarmups()
    {
        return Arrays.asList(warmups);
    }

}
