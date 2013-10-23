package org.apache.cassandra.stress.generatedata;

import org.apache.cassandra.utils.FBUtilities;

import java.io.File;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

import static com.google.common.base.Charsets.*;

public class DataGenStringDictionary extends DataGen
{

    private final byte space = ' ';

    public DataGenStringDictionary(File dictionary)
    {

    }

    @Override
    public void generate(ByteBuffer fill, long index)
    {
        fill(fill, index, 0);
    }

    @Override
    public void generate(List<ByteBuffer> fills, long index)
    {
        for (int i = 0 ; i < fills.size() ; i++)
        {
            fill(fills.get(0), index, i);
        }
    }

    private void fill(ByteBuffer fill, long index, int column)
    {
        fill.clear();
        byte[] trg = fill.array();
        byte[] src = getData(index, column);
        for (int j = 0 ; j < trg.length ; j += src.length)
            System.arraycopy(src, 0, trg, j, Math.min(src.length, trg.length - j));
    }

    private byte[] getData(long index, int column)
    {
        final long key = (column * repeatFrequency) + (index % repeatFrequency);
        byte[] r = cache.get(key);
        if (r != null)
            return r;
        MessageDigest md = FBUtilities.threadLocalMD5Digest();
        r = md.digest(Long.toString(key).getBytes(UTF_8));
        cache.putIfAbsent(key, r);
        return r;
    }

    @Override
    public boolean deterministic()
    {
        return true;
    }

    public static DataGenFactory getFactory(File file)
    {

    }

}
