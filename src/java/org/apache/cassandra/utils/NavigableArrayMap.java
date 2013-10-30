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
    final K[] keys;
    final V[] values;

    public NavigableArrayMap(Comparator<K> cmp, K[] keys, V[] values)
    {
        this.cmp = cmp;
        this.keys = keys;
        this.values = values;
    }

    public NavigableArrayMap(Comparator<K> cmp)
    {
        this.cmp = cmp;
        this.keys = (K[]) EMPTY;
        this.values = (V[]) EMPTY;
    }


    @Override
    public V get(Object key)
    {

        return null;
    }

    @Override
    public Entry<K, V> lowerEntry(K key)
    {
        int i = Arrays.binarySearch(keys, key, comparator());
        return null;
    }

    @Override
    public K lowerKey(K key)
    {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public Entry<K, V> floorEntry(K key)
    {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public K floorKey(K key)
    {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public Entry<K, V> ceilingEntry(K key)
    {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public K ceilingKey(K key)
    {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public Entry<K, V> higherEntry(K key)
    {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public K higherKey(K key)
    {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public Entry<K, V> firstEntry()
    {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public Entry<K, V> lastEntry()
    {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public Entry<K, V> pollFirstEntry()
    {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public Entry<K, V> pollLastEntry()
    {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public NavigableMap<K, V> descendingMap()
    {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public NavigableSet<K> navigableKeySet()
    {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public NavigableSet<K> descendingKeySet()
    {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public NavigableMap<K, V> subMap(K fromKey, boolean fromInclusive, K toKey, boolean toInclusive)
    {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public NavigableMap<K, V> headMap(K toKey, boolean inclusive)
    {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public NavigableMap<K, V> tailMap(K fromKey, boolean inclusive)
    {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public Comparator<? super K> comparator()
    {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public SortedMap<K, V> subMap(K fromKey, K toKey)
    {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public SortedMap<K, V> headMap(K toKey)
    {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public SortedMap<K, V> tailMap(K fromKey)
    {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public K firstKey()
    {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public K lastKey()
    {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public int size()
    {
        return 0;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public boolean isEmpty()
    {
        return false;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public boolean containsKey(Object key)
    {
        return false;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public boolean containsValue(Object value)
    {
        return false;  //To change body of implemented methods use File | Settings | File Templates.
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
    public void putAll(Map<? extends K, ? extends V> m)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void clear()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Set<K> keySet()
    {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public Collection<V> values()
    {
        return Arrays.asList(values);
    }

    @Override
    public Set<Entry<K, V>> entrySet()
    {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

}
