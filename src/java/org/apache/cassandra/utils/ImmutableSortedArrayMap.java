package org.apache.cassandra.utils;

import java.util.AbstractMap;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.Map;
import java.util.NavigableMap;
import java.util.NavigableSet;
import java.util.Set;
import java.util.SortedMap;

public class ImmutableSortedArrayMap<K, V>
{

    static final Object[] EMPTY = new Object[0];

    final K[] keys;
    final V[] values;

    public ImmutableSortedArrayMap(final K[] keys, final V[] values, final Comparator<K> cmp)
    {
        // sorts the array
        // TODO : avoid allocating Integer[]
        final Integer[] proxy = new Integer[keys.length];
        for (int i = 0 ; i != proxy.length ; i++)
            proxy[i] = i;
        Arrays.sort(proxy, new Comparator<Integer>(){

            @Override
            public int compare(Integer o1, Integer o2)
            {
                return cmp.compare(keys[o1], keys[o2]);
            }
        });
        int offset = 0;
        for (int i = 0 ; i < keys.length ; i++)
        {
        }
        this.keys = keys;
        this.values = values;
    }

    public ImmutableSortedArrayMap(K[] keys, V[] values)
    {
        // assumes sorted
        this.keys = keys;
        this.values = values;
    }

    public ImmutableSortedArrayMap()
    {
        this.keys = (K[]) EMPTY;
        this.values = (V[]) EMPTY;
    }

    public ImmutableSortedArrayMap<K, V> merge(K[] keys, V[] values)
    {
        // TODO: my optimisation from years back
        throw new UnsupportedOperationException();
    }

    public int lowerIndex(K key, Comparator<K> cmp)
    {
        int i = Arrays.binarySearch(keys, key, cmp);
        if (i < 0)
            return -i-2;
        return i - 1;
    }

    public int floorIndex(K key, Comparator<K> cmp)
    {
        int i = Arrays.binarySearch(keys, key, cmp);
        if (i < 0)
            return -i-1 < keys.length ? -i-1 : -1;
        return i;
    }

    public int ceilIndex(K key, Comparator<K> cmp)
    {
        int i = Arrays.binarySearch(keys, key, cmp);
        if (i < 0)
            return -i-2;
        return i;
    }

    public int higherIndex(K key, Comparator<K> cmp)
    {
        int i = Arrays.binarySearch(keys, key, cmp);
        if (i < 0)
            return -i-1 < keys.length ? -i-1 : -1;
        return i + 1;
    }

    public K getKey(int i)
    {
        return keys[i];
    }

    public V getValue(int i)
    {
        return values[i];
    }

    public Map.Entry<K, V> getEntry(int i)
    {
        return new AbstractMap.SimpleImmutableEntry<>(getKey(i), getValue(i));
    }

}
