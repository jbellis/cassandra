package org.apache.cassandra.utils.btree;

import java.util.Comparator;
import java.util.Iterator;

import static org.apache.cassandra.utils.btree.BTree.MAX_DEPTH;
import static org.apache.cassandra.utils.btree.BTree.NEGATIVE_INFINITY;
import static org.apache.cassandra.utils.btree.BTree.POSITIVE_INFINITY;
import static org.apache.cassandra.utils.btree.BTree.getLeafKeyEnd;
import static org.apache.cassandra.utils.btree.BTree.isLeaf;

/**
 * An extension of Stack which provides a public interface for iterating over or counting a subrange of the tree
 *
 * @param <V>
 */
public final class Cursor<V> extends Stack implements Iterator<V>
{

    /**
     * Returns a cursor that can be reused to iterate over trees
     * @param <V>
     * @return
     */
    static <V> Cursor<V> newCursor()
    {
        // try to encourage stack allocation - may be misguided. but no harm
        Object[][] stack = new Object[MAX_DEPTH][];
        byte[] index = new byte[MAX_DEPTH];
        return new Cursor(stack, index);
    }

    byte endIndex;
    Object[] endNode;
    boolean forwards;

    private Cursor(Object[][] stack, byte[] index)
    {
        super(stack, index);
    }

    /**
     * Reset this cursor for the provided tree, to iterate over its entire range
     * @param btree the tree to iterate over
     * @param forwards if false, the cursor will start at the end and move backwards
     */
    public void reset(Object[] btree, boolean forwards)
    {
        _reset(btree, null, NEGATIVE_INFINITY, false, POSITIVE_INFINITY, false, forwards);
    }

    /**
     * Reset this cursor for the provided tree, to iterate between the provided start and end
     * @param btree the tree to iterate over
     * @param comparator  the comparator that defines the ordering over the items in the tree
     * @param lowerBound the first item to include, inclusive
     * @param upperBound the last item to include, exclusive
     * @param forwards if false, the cursor will start at the end and move backwards
     */
    public void reset(Object[] btree, Comparator<V> comparator, V lowerBound, V upperBound, boolean forwards)
    {
        _reset(btree, comparator, lowerBound, true, upperBound, false, forwards);
    }

    /**
     * Reset this cursor for the provided tree, to iterate between the provided start and end
     * @param btree the tree to iterate over
     * @param comparator  the comparator that defines the ordering over the items in the tree
     * @param lowerBound the first item to include
     * @param inclusiveLowerBound should include start in the iterator, if present in the tree
     * @param upperBound the last item to include
     * @param inclusiveUpperBound should include end in the iterator, if present in the tree
     * @param forwards if false, the cursor will start at the end and move backwards
     */
    public void reset(Object[] btree, Comparator<V> comparator, V lowerBound, boolean inclusiveLowerBound, V upperBound, boolean inclusiveUpperBound, boolean forwards)
    {
        _reset(btree, comparator, lowerBound, inclusiveLowerBound, upperBound, inclusiveUpperBound, forwards);
    }

    private void _reset(Object[] btree, Comparator<V> comparator, Object lowerBound, boolean inclusiveLowerBound, Object upperBound, boolean inclusiveUpperBound, boolean forwards)
    {
        if (lowerBound == null)
            lowerBound = NEGATIVE_INFINITY;
        if (upperBound == null)
            upperBound = POSITIVE_INFINITY;

        this.forwards = forwards;

        Stack findLast = Stack.newStack();
        if (forwards)
        {
            findLast.find(btree, comparator, upperBound, inclusiveUpperBound ? Find.HIGHER : Find.CEIL, true);
            find(btree, comparator, lowerBound, inclusiveLowerBound ? Find.CEIL : Find.HIGHER, true);
        }
        else
        {
            findLast.find(btree, comparator, lowerBound, inclusiveLowerBound ? Find.LOWER : Find.FLOOR, false);
            find(btree, comparator, upperBound, inclusiveUpperBound ? Find.FLOOR : Find.LOWER, false);
        }
        int c = this.compareTo(findLast, forwards);
        if (forwards ? c > 0 : c < 0)
        {
            endNode = stack[0] = null;
            endIndex = index[0] = 0;
            depth = 0;
        }
        else
        {
            endNode = findLast.stack[findLast.depth];
            endIndex = findLast.index[findLast.depth];
        }
    }

    @Override
    public boolean hasNext()
    {
        return stack[depth] != endNode || index[depth] != endIndex;
    }

    @Override
    public V next()
    {
        Object[] cur = stack[depth];
        int curi = index[depth];
        Object r = cur[curi];
        if (forwards)
            successor(cur, curi);
        else
            predecessor(cur, curi);
        return (V) r;
    }

    public int count()
    {
        assert forwards;
        int count = 0;
        int next;
        while ((next = consumeNextLeaf()) >= 0)
            count += next;
        return count;
    }

    private int consumeNextLeaf()
    {
        Object[] cur = stack[depth];
        int r = 0;
        if (!isLeaf(cur))
        {
            int curi = index[depth];
            if (cur == endNode && curi == endIndex)
                return -1;
            r = 1;
            successor(cur, curi);
            cur = stack[depth];
        }
        if (cur == endNode)
        {
            if (index[depth] == endIndex)
                return -1;
            r += endIndex - index[depth];
            index[depth] = endIndex;
            return r;
        }
        int keyEnd = getLeafKeyEnd(cur);
        r += keyEnd - index[depth];
        successor(cur, keyEnd);
        return r;
    }

    @Override
    public void remove()
    {
        // currently does nothing!
    }
}
