package org.apache.cassandra.utils;

import java.util.AbstractCollection;
import java.util.AbstractMap;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Map;
import java.util.SortedMap;

public class ImmutableSortedArrayMap<K, V>
{

    static final ImmutableSortedArrayMap EMPTY = new ImmutableSortedArrayMap(new Object[0]);

    final Object[] map;

    // must be sorted!
    public ImmutableSortedArrayMap(SortedMap<K, V> from)
    {
        this.map = new Object[from.size() << 1];
        int i = 0;
        for (Map.Entry<K, V> e : from.entrySet())
        {
            map[i] = e.getKey();
            map[i] = e.getValue();
            i += 2;
        }
    }

    public ImmutableSortedArrayMap()
    {
        this.map = EMPTY.map;
    }

    protected ImmutableSortedArrayMap(ImmutableSortedArrayMap<K, V> that)
    {
        this.map = that.map;
    }

    protected ImmutableSortedArrayMap(Object[] map)
    {
        this.map = map;
    }

    public ImmutableSortedArrayMap<K, V> update(Comparator<? super K> cmp, ImmutableSortedArrayMap<K, V> that)
    {
        // for now, a fairly rubbish linear implementation
        if (this.map.length == that.map.length)
        {
            boolean same = true;
            for (int i = 0 ; same && i < map.length ; i++)
                same = cmp.compare((K) map[i], (K) that.map[i]) == 0;
            if (same)
                return that;
        }

        Object[] r = new Object[this.map.length + that.map.length];
        int i1 = 0, i2 = 0, ir = 0;
        while (i1 < this.map.length && i2 < that.map.length)
        {
            int c = cmp.compare((K) this.map[i1], (K) that.map[i2]);
            if (c >= 0)
            {
                r[ir] = that.map[i2];
                r[ir + 1] = that.map[i2 + 1];
                i2 += 2;
                if (c == 0)
                    i1 += 2;
            }
            else
            {
                r[ir] = this.map[i1];
                r[ir + 1] = this.map[i1 + 1];
                i1 += 2;
            }
            ir += 2;
        }

        if (i1 < this.map.length)
        {
            System.arraycopy(this.map, i1, r, ir, this.map.length - i1);
            ir += this.map.length - i1;
        }
        if (i2 < that.map.length)
        {
            System.arraycopy(that.map, i2, r, ir, that.map.length - i2);
            ir += that.map.length - i2;
        }

        if (ir < r.length)
            r = Arrays.copyOf(r, ir);

        return new ImmutableSortedArrayMap<>(r);
    }

    public int index(K key, Comparator<K> cmp)
    {
        return equal(cmp, key, 0, length());
    }

    public int lowerIndex(K key, Comparator<K> cmp)
    {
        return lower(cmp, key, 0, length());
    }

    public int floorIndex(K key, Comparator<K> cmp)
    {
        return floor(cmp, key, 0, length());
    }

    public int ceilIndex(K key, Comparator<K> cmp)
    {
        return ceil(cmp, key, 0, length());
    }

    public int higherIndex(K key, Comparator<K> cmp)
    {
        return higher(cmp, key, 0, length());
    }

    private int equal(Comparator<K> cmp, final K key, final int fromIndex, final int toIndex) {

        int i = fromIndex - 1;
        int j = toIndex;
        // a[-1] ^= -infinity

        while (i < j - 1) {

            // { a[i] <= v ^ a[j] > v }

            final int m = (i + j) >>> 1;
            final K k = (K) map[m << 1];

            int c = cmp.compare(k, key);
            if (c < 0) i = m;
            else if (c == 0) return m;
            else j = m;

            // { a[m] > v  =>        a[j] > v        =>      a[i] <= v ^ a[j] > v }
            // { a[m] <= v =>        a[i] <= v       =>      a[i] <= v ^ a[j] > v }

        }

        return -1;
    }

    private int floor(Comparator<K> cmp, final K key, final int fromIndex, final int toIndex) {

        int i = fromIndex - 1;
        int j = toIndex;
        // a[-1] ^= -infinity

        while (i < j - 1) {

            // { a[i] <= v ^ a[j] > v }

            final int m = (i + j) >>> 1;
            final K k = (K) map[m << 1];

            if (cmp.compare(k, key) <= 0) i = m;
            else j = m;

            // { a[m] > v  =>        a[j] > v        =>      a[i] <= v ^ a[j] > v }
            // { a[m] <= v =>        a[i] <= v       =>      a[i] <= v ^ a[j] > v }

        }

        return i;
    }

    private int ceil(Comparator<? super K> cmp, final K key, final int fromIndex, final int toIndex) {


        int i = fromIndex -1;
        int j = toIndex;

        while (i < j - 1) {

            // { a[i] < v ^ a[j] >= v }

            final int m = (i + j) >>> 1;
            final K k = (K) map[m << 1];

            if (cmp.compare(k, key) >= 0) j = m;
            else i = m;

            // { a[m] >= v  =>        a[j] >= v       =>      a[i] < v ^ a[j] >= v }
            // { a[m] < v   =>        a[i] < v        =>      a[i] < v ^ a[j] >= v }

        }

        if (j == toIndex)
            return -1;
        return j;

    }

    // greatest index of value that is lower than key, -1 if none
    private int lower(Comparator<? super K> cmp, final K key, int fromIndex, int toIndex) {

        int i = fromIndex - 1;
        int j = toIndex;
        // a[-1] ^= -infinity

        while (i < j - 1) {

            // { a[i] <= v ^ a[j] > v }

            final int m = (i + j) >>> 1;
            final K k = (K) map[m << 1];

            if (cmp.compare(k, key) < 0) i = m;
            else j = m;

            // { a[m] > v  =>        a[j] > v        =>      a[i] <= v ^ a[j] > v }
            // { a[m] <= v =>        a[i] <= v       =>      a[i] <= v ^ a[j] > v }

        }

        return i;
    }

    // lowest index of value that is greater than key, -1 if none
    private int higher(Comparator<? super K> cmp, final K key, final int fromIndex, final int toIndex) {


        int i = fromIndex -1;
        int j = toIndex;

        while (i < j - 1) {

            // { a[i] < v ^ a[j] >= v }

            final int m = (i + j) >>> 1;
            final K k = (K) map[m << 1];

            if (cmp.compare(k, key) > 0) j = m;
            else i = m;

            // { a[m] >= v  =>        a[j] >= v       =>      a[i] < v ^ a[j] >= v }
            // { a[m] < v   =>        a[i] < v        =>      a[i] < v ^ a[j] >= v }

        }

        if (j == toIndex)
            return -1;
        return j;

    }

    public K getKey(int i)
    {
        return i == -1 ? null : (K) map[i << 1];
    }

    public V getValue(int i)
    {
        return i == -1 ? null : (V) map[1 + (i << 1)];
    }

    public Map.Entry<K, V> getEntry(int i)
    {
        return i == -1 ? null : new AbstractMap.SimpleImmutableEntry<>(getKey(i), getValue(i));
    }

    public int length()
    {
        return  map.length >> 1;
    }

    public Collection<K> keys(boolean asc)
    {
        return asc
                ? this.<K>collection(0, 2)
                : this.<K>collection(map.length - 2, -2);
    }

    public Collection<V> values(boolean keyAsc)
    {
        return keyAsc
                ? this.<V>collection(1, 2)
                : this.<V>collection(map.length - 1, -2);
    }

    private <T> Collection<T> collection(final int start, final int delta)
    {
        return new AbstractCollection<T>()
        {
            @Override
            public Iterator<T> iterator()
            {
                return new Iterator<T>()
                {
                    int p = start;
                    @Override
                    public boolean hasNext()
                    {
                        return p >= 0 && p < map.length;
                    }

                    @Override
                    public T next()
                    {
                        T r = (T) map[p];
                        p += delta;
                        return r;
                    }

                    @Override
                    public void remove()
                    {
                        throw new UnsupportedOperationException();
                    }
                };
            }

            @Override
            public int size()
            {
                return length();
            }
        };
    }


}
