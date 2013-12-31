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
public final class Cursor<V> extends Path implements Iterator<V>
{
    /**
     * Returns a cursor that can be reused to iterate over trees
     *
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

    // the last node covered by the requested range
    private Object[] endNode;
    // the index within endNode that signals we're finished -- that is, endNode[endIndex] is NOT part of the Cursor
    private byte endIndex;

    private boolean forwards;

    private Cursor(Object[][] stack, byte[] index)
    {
        super(stack, index);
    }

    /**
     * Reset this cursor for the provided tree, to iterate over its entire range
     *
     * @param btree    the tree to iterate over
     * @param forwards if false, the cursor will start at the end and move backwards
     */
    public void reset(Object[] btree, boolean forwards)
    {
        _reset(btree, null, NEGATIVE_INFINITY, false, POSITIVE_INFINITY, false, forwards);
    }

    /**
     * Reset this cursor for the provided tree, to iterate between the provided start and end
     *
     * @param btree      the tree to iterate over
     * @param comparator the comparator that defines the ordering over the items in the tree
     * @param lowerBound the first item to include, inclusive
     * @param upperBound the last item to include, exclusive
     * @param forwards   if false, the cursor will start at the end and move backwards
     */
    public void reset(Object[] btree, Comparator<V> comparator, V lowerBound, V upperBound, boolean forwards)
    {
        _reset(btree, comparator, lowerBound, true, upperBound, false, forwards);
    }

    /**
     * Reset this cursor for the provided tree, to iterate between the provided start and end
     *
     * @param btree               the tree to iterate over
     * @param comparator          the comparator that defines the ordering over the items in the tree
     * @param lowerBound          the first item to include
     * @param inclusiveLowerBound should include start in the iterator, if present in the tree
     * @param upperBound          the last item to include
     * @param inclusiveUpperBound should include end in the iterator, if present in the tree
     * @param forwards            if false, the cursor will start at the end and move backwards
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

        Path findLast = Path.newPath();
        if (forwards)
        {
            findLast.find(btree, comparator, upperBound, inclusiveUpperBound ? Op.HIGHER : Op.CEIL, true);
            find(btree, comparator, lowerBound, inclusiveLowerBound ? Op.CEIL : Op.HIGHER, true);
        }
        else
        {
            findLast.find(btree, comparator, lowerBound, inclusiveLowerBound ? Op.LOWER : Op.FLOOR, false);
            find(btree, comparator, upperBound, inclusiveUpperBound ? Op.FLOOR : Op.LOWER, false);
        }
        int c = this.compareTo(findLast, forwards);
        if (forwards ? c > 0 : c < 0)
        {
            endNode = path[depth];
            endIndex = indexes[depth];
        }
        else
        {
            endNode = findLast.path[findLast.depth];
            endIndex = findLast.indexes[findLast.depth];
        }
    }

    public boolean hasNext()
    {
        return path[depth] != endNode || indexes[depth] != endIndex;
    }

    public V next()
    {
        Object r = currentKey();
        if (forwards)
            successor();
        else
            predecessor();
        return (V) r;
    }

    public int count()
    {
        if (!forwards)
            throw new IllegalStateException("Count can only be run on forward cursors");
        int count = 0;
        int next;
        while ((next = consumeNextLeaf()) >= 0)
            count += next;
        return count;
    }

    /**
     * @return
     */
    private int consumeNextLeaf()
    {
        Object[] node = currentNode();
        int r = 0;
        if (!isLeaf(node))
        {
            int i = currentIndex();
            if (node == endNode && i == endIndex)
                return -1;
            r = 1;
            successor();
            node = currentNode();
        }
        if (node == endNode)
        {
            if (currentIndex() == endIndex)
                return r > 0 ? r : -1;
            r += endIndex - currentIndex();
            setIndex(endIndex);
            return r;
        }
        int keyEnd = getLeafKeyEnd(node);
        r += keyEnd - currentIndex();
        setIndex(keyEnd);
        successor();
        return r;
    }

    public void remove()
    {
        throw new UnsupportedOperationException();
    }
}
