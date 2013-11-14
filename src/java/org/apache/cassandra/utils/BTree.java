package org.apache.cassandra.utils;

import java.util.Collection;

public class BTree<V extends Comparable<V>>
{

    private static final int FAN_FACTOR = 64;
    private static final BTree EMPTY = new BTree();

    public static final <V extends  Comparable<V>> BTree<V> empty()
    {
        return EMPTY;
    }

    /**
     * Leaf Nodes: Object[value, value, ...]   ALWAYS ODD NUMBER OF ELEMENTS
     * Branch Nodes: Object[key, key, ..., leaf, leaf, ...]   ALWAYS EVEN NUMBER OF ELEMENTS
     */

    private Object[] head;
    private Object id = new Object();

    private BTree()
    {
        head = new Object[0];
    }

    private BTree(BTree<V> copy)
    {
        head = copy.head;
    }

    public BTree<V> copy()
    {
        return new BTree(this);
    }

    public void update(Collection<V> vs)
    {
        int depth = depth(head);
    }

//    public

    private static int depth(Object[] node)
    {
        int c = 1;
        while (!isLeaf(node))
        {
            node = (Object[]) node[0];
            c++;
        }
        return c;
    }

    private static boolean isLeaf(Object[] node)
    {
        return (node.length & 1) == 1;
    }

}
