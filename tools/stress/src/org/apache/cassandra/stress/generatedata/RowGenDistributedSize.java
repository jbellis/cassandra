package org.apache.cassandra.stress.generatedata;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;

public class RowGenDistributedSize extends RowGen
{

    static final int MAX_BUFFER_CACHE_SIZE = 1024 * 1024;
    static final int MAX_COLUMN_COUNT = 1024;

    final Distribution countDistribution;
    final Distribution sizeDistribution;

    final LinkedHashMap<CacheKey, CachedBuffer> cache = new LinkedHashMap<>();
    int cacheSize;

    static final class CachedBuffer
    {
        final ByteBuffer buffer;
        final CacheKey key;
        boolean using;

        CachedBuffer(ByteBuffer buffer, CacheKey key)
        {
            this.buffer = buffer;
            this.key = key;
        }
    }

    static final class CacheKey
    {
        final int size;
        final int offset;

        CacheKey(int size, int offset)
        {
            this.size = size;
            this.offset = offset;
        }

        @Override
        public int hashCode()
        {
            return (size * 31) + offset;
        }

        @Override
        public boolean equals(Object that)
        {
            return that instanceof CacheKey & equals((CacheKey) that);
        }

        public boolean equals(CacheKey that)
        {
            return this.size == that.size && this.offset == that.offset;
        }
        public CacheKey dup()
        {
            return new CacheKey(size, offset);
        }
    }

    // array re-used for returning columns
    final ByteBuffer[] ret;
    final CachedBuffer[] cached;

    public RowGenDistributedSize(DataGen dataGenerator, Distribution countDistribution, Distribution sizeDistribution)
    {
        super(dataGenerator);
        this.countDistribution = countDistribution;
        this.sizeDistribution = sizeDistribution;
        ret = new ByteBuffer[Math.min(MAX_COLUMN_COUNT, (int) sizeDistribution.maxValue())];
        cached = new CachedBuffer[ret.length];
    }

    @Override
    List<ByteBuffer> getColumns(long operationIndex)
    {
        int i = 0;
        int count = (int) countDistribution.next();
        while (i < count)
        {
            int columnSize = (int) sizeDistribution.next();
            CachedBuffer buf = null;
            int offset = 0;
            while (buf == null)
            {
                CacheKey key = new CacheKey(columnSize, offset);
                buf = cache.get(key);
                if (buf == null)
                {
                    buf = new CachedBuffer(ByteBuffer.allocate(columnSize), key.dup());
                    cacheSize += columnSize;
                    if (cacheSize > MAX_BUFFER_CACHE_SIZE)
                    {
                        Iterator<CacheKey> iter = cache.keySet().iterator();
                        while (cacheSize > MAX_BUFFER_CACHE_SIZE)
                        {
                            cacheSize -= iter.next().size;
                            iter.remove();
                        }
                    }
                }
                else if (buf.using)
                    buf = null;
                offset++;
            }
            cached[i] = buf;
            buf.using = true;
            cache.put(buf.key, buf);
            ret[i] = buf.buffer;
            i++;
        }
        for ( i = 0 ; i < count ; i++ )
            cached[i].using = false;
        return Arrays.asList(ret).subList(0, count);
    }

    @Override
    public boolean isDeterministic()
    {
        return false;
    }

}
