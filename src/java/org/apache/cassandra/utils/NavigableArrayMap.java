package org.apache.cassandra.utils;

import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.Map;
import java.util.NavigableMap;
import java.util.NavigableSet;
import java.util.Set;
import java.util.SortedMap;

public class NavigableArrayMap<K, V>  implements NavigableMap<K, V>
{

    static final Object[] EMPTY = new Object[0];

    final Comparator<K> cmp;
    volatile ImmutableSortedArrayMap<K, V> map = ImmutableSortedArrayMap.EMPTY;

    public NavigableArrayMap(Comparator<K> cmp)
    {
        this.cmp = cmp;
    }

    @Override
    public V get(Object key)
    {
        return map.getValue(map.index((K) key, cmp));
    }

    @Override
    public Entry<K, V> lowerEntry(K key)
    {
        return map.getEntry(map.lowerIndex(key, cmp));
    }

    @Override
    public Entry<K, V> floorEntry(K key)
    {
        return map.getEntry(map.floorIndex(key, cmp));
    }

    @Override
    public Entry<K, V> ceilingEntry(K key)
    {
        return map.getEntry(map.ceilIndex(key, cmp));
    }

    @Override
    public Entry<K, V> higherEntry(K key)
    {
        return map.getEntry(map.higherIndex(key, cmp));
    }

    @Override
    public K lowerKey(K key)
    {
        return map.getKey(map.lowerIndex(key, cmp));
    }

    @Override
    public K floorKey(K key)
    {
        return map.getKey(map.floorIndex(key, cmp));
    }

    @Override
    public K ceilingKey(K key)
    {
        return map.getKey(map.ceilIndex(key, cmp));
    }

    @Override
    public K higherKey(K key)
    {
        return map.getKey(map.higherIndex(key, cmp));
    }

    @Override
    public Entry<K, V> firstEntry()
    {
        return map.length() > 0 ? map.getEntry(0) : null;
    }

    @Override
    public Entry<K, V> lastEntry()
    {
        return map.length() > 0 ? map.getEntry(map.length() - 1) : null;
    }

    @Override
    public K firstKey()
    {
        return map.length() > 0 ? map.getKey(0) : null;
    }

    @Override
    public K lastKey()
    {
        return map.length() > 0 ? map.getKey(map.length() - 1) : null;
    }

    @Override
    public Entry<K, V> pollFirstEntry()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Entry<K, V> pollLastEntry()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public NavigableMap<K, V> descendingMap()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public NavigableSet<K> navigableKeySet()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public NavigableSet<K> descendingKeySet()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public NavigableMap<K, V> subMap(K fromKey, boolean fromInclusive, K toKey, boolean toInclusive)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public NavigableMap<K, V> headMap(K toKey, boolean inclusive)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public NavigableMap<K, V> tailMap(K fromKey, boolean inclusive)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Comparator<? super K> comparator()
    {
        return cmp;
    }

    @Override
    public SortedMap<K, V> subMap(K fromKey, K toKey)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public SortedMap<K, V> headMap(K toKey)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public SortedMap<K, V> tailMap(K fromKey)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public int size()
    {
        return map.length();
    }

    @Override
    public boolean isEmpty()
    {
        return map.length() == 0;
    }

    @Override
    public boolean containsKey(Object key)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean containsValue(Object value)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public V put(K key, V value)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public V remove(Object key)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Set<K> keySet()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void putAll(Map<? extends K, ? extends V> m)
    {
    }

    @Override
    public void clear()
    {
        map = ImmutableSortedArrayMap.EMPTY;
    }

    @Override
    public Collection<V> values()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Set<Entry<K, V>> entrySet()
    {
        throw new UnsupportedOperationException();
    }

}
