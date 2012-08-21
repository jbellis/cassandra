package org.apache.cassandra.tracing;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.apache.cassandra.thrift.ColumnParent;
import org.apache.cassandra.thrift.SlicePredicate;
import org.apache.cassandra.thrift.SliceRange;

import static org.apache.cassandra.utils.ByteBufferUtil.bytesToHex;

public class TraceParameters
{
    public static String toString(ColumnParent column_parent)
    {
        String st = column_parent.column_family;
        if (column_parent.super_column != null)
            st += ":" + bytesToHex(column_parent.super_column);
        return st;
    }

    public static String toString(SlicePredicate predicate)
    {
        SliceRange sr = predicate.slice_range;
        if (sr != null)
        {
            return String.format("SlicePredicate(range=%s..%s, count=%s, reversed=%s)",
                                 bytesToHex(sr.start), bytesToHex(sr.finish), sr.count, sr.reversed);
        }

        List<ByteBuffer> names = predicate.column_names;
        List<String> hexNames = new ArrayList<String>(names.size());
        for (ByteBuffer name : names)
            hexNames.add(bytesToHex(name));
        return String.format("SlicePredicate(names=%s)", hexNames);
    }
}
