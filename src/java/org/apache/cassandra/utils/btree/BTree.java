package org.apache.cassandra.utils.btree;

import com.google.common.base.*;
import org.apache.pig.builtin.FLOOR;

import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;

public class BTree
{

    /**
     * Leaf Nodes: Object[key:A, key:B, ...,]   ALWAYS EVEN NUMBER OF ELEMENTS, last one possibly null as padding to ensure even
     * Branch Nodes: Object[key:A, key:B..., child[&lt;A], child[&lt;B], ..., child[&lt; Inf]]   ALWAYS ODD NUMBER OF ELEMENTS
     *
     *
     */

    // The maximum fan factor used for B-Trees
    static final int FAN_SHIFT = 5;
    static final int FAN_FACTOR = 1 << FAN_SHIFT;
    static final int QUICK_MERGE_LIMIT = Math.min(FAN_FACTOR, 16) * 2;

    // Maximum depth of any B-Tree. In reality this is just an arbitrary limit, and is currently imposed on iterators only,
    // but a maximum depth sufficient to store at worst Integer.MAX_VALUE items seems reasonable
    // 2^n = (2^k).(2^(n/k)) => 2^31 <= 2^(FAN_SHIFT-1) . 2^ceil(31 / (FAN_SHIFT - 1))
    static final int MAX_DEPTH = (int) Math.ceil(31d / (FAN_SHIFT - 1));

    // An empty BTree Leaf - which is the same as an empty BTree
    static final Object[] EMPTY_LEAF = new Object[0];

    // An empty BTree branch - used only for internal purposes in BTreeModifier
    static final Object[] EMPTY_BRANCH = new Object[1];

    /**
     * Returns an empty BTree
     * @return
     */
    public static Object[] empty()
    {
        return EMPTY_LEAF;
    }

    /**
     * returns a BTree containing all of the provided collection
     * @param src the items to build the tree with
     * @param comparator the comparator that defines the ordering over the items in the tree
     * @param sorted if false, the collection will be copied and sorted to facilitate construction
     * @param <V>
     * @return
     */
    public static <V> Object[] build(Collection<V> src, Comparator<V> comparator, boolean sorted)
    {
        int size = src.size();

        if (size < FAN_FACTOR)
        {
            V[] vs = src.toArray((V[]) new Object[size + (size & 1)]);
            Arrays.sort(vs, 0, size, comparator);
            return vs;
        }

        if (!sorted)
            src = sorted(src, comparator, size);

        return MODIFIER.get().build(src, size);
    }

    /**
     * Returns a new BTree with the provided set inserting/replacing as necessary any equal items
     * @param btree the tree to update
     * @param comparator the comparator that defines the ordering over the items in the tree
     * @param updateWith the items to either insert / update
     * @param updateWithIsSorted if false, updateWith will be copied and sorted to facilitate construction
     * @param <V>
     * @return
     */
    public static <V> Object[] update(
            Object[] btree,
            Comparator<V> comparator,
            Collection<V> updateWith,
            boolean updateWithIsSorted
    )
    {
        return update(btree, comparator, updateWith, updateWithIsSorted, null, null);
    }

    /**
     * Returns a new BTree with the provided set inserting/replacing as necessary any equal items
     * @param btree the tree to update
     * @param comparator the comparator that defines the ordering over the items in the tree
     * @param updateWith the items to either insert / update
     * @param updateWithIsSorted if false, updateWith will be copied and sorted to facilitate construction
     * @param replaceF a function to apply to a pair we are swapping
     * @param terminateEarly a function to call with any argument that returns Boolean.TRUE if we should terminate before finishing our work
     * @param <V>
     * @return
     */
    public static <V> Object[] update(
            Object[] btree,
            Comparator<V> comparator,
            Collection<V> updateWith,
            boolean updateWithIsSorted,
            ReplaceFunction<V> replaceF,
            Function<?, Boolean> terminateEarly
    )
    {
        if (btree.length == 0)
            return build(updateWith, comparator, updateWithIsSorted);

        if (!updateWithIsSorted)
            updateWith = sorted(updateWith, comparator, updateWith.size());

        if (isLeaf(btree) && btree.length + updateWith.size() < QUICK_MERGE_LIMIT)
        {
            // try a quick in-place merge
            int btreeOffset = 0;
            int keyEnd = getLeafKeyEnd(btree);
            Object[] merged = new Object[QUICK_MERGE_LIMIT];
            int mergedCount = 0;
            for (V v : updateWith)
            {
                int p = find(comparator, v, btree, btreeOffset, keyEnd);
                boolean found = p >= 0;
                if (!found)
                    p = -p - 1;
                int count = p - btreeOffset;
                if (count > 0)
                {
                    System.arraycopy(btree, btreeOffset, merged, mergedCount, count);
                    mergedCount += count;
                    btreeOffset = p;
                }
                if (found)
                {
                    btreeOffset++;
                    if (replaceF != null)
                        v = replaceF.apply((V) btree[p], v);
                } else if (replaceF != null)
                    v = replaceF.apply(null, v);

                merged[mergedCount++] = v;
            }
            if (btreeOffset < keyEnd)
            {
                int count = keyEnd - btreeOffset;
                System.arraycopy(btree, btreeOffset, merged, mergedCount, count);
                mergedCount += count;
            }
            if (mergedCount > FAN_FACTOR)
            {
                int mid = (mergedCount >> 1) & ~1;
                return new Object[] {
                        merged[mid],
                        Arrays.copyOfRange(merged, 0, mid),
                        Arrays.copyOfRange(merged, 1 + mid, mergedCount + ((mergedCount + 1) & 1)),
                };
            }
            return Arrays.copyOfRange(merged, 0, mergedCount + (mergedCount & 1));
        }

        return MODIFIER.get().update(btree, comparator, updateWith, replaceF, terminateEarly);
    }

    /**
     * Returns an Iterator over the entire tree
     *
     * @param btree the tree to iterate over
     * @param forwards if false, the iterator will start at the end and move backwards
     * @param <V>
     * @return
     */
    public static <V> Cursor<V> slice(Object[] btree, boolean forwards)
    {
        Cursor<V> r = Cursor.newCursor();
        r.reset(btree, forwards);
        return r;
    }

    /**
     * Returns an Iterator over a sub-range of the tree
     *
     * @param btree the tree to iterate over
     * @param comparator  the comparator that defines the ordering over the items in the tree
     * @param start the first item to include
     * @param end the last item to include
     * @param forwards if false, the iterator will start at end and move backwards
     * @param <V>
     * @return
     */
    public static <V> Cursor<V> slice(Object[] btree, Comparator<V> comparator, V start, V end, boolean forwards)
    {
        Cursor<V> r = Cursor.newCursor();
        r.reset(btree, comparator, start, end, forwards);
        return r;
    }

    /**
     * Returns an Iterator over a sub-range of the tree
     *
     * @param btree the tree to iterate over
     * @param comparator  the comparator that defines the ordering over the items in the tree
     * @param start the first item to include
     * @param end the last item to include
     * @param forwards if false, the iterator will start at end and move backwards
     * @param <V>
     * @return
     */
    public static <V> Cursor<V> slice(Object[] btree, Comparator<V> comparator, V start, boolean startInclusive, V end, boolean endInclusive, boolean forwards)
    {
        Cursor<V> r = Cursor.newCursor();
        r.reset(btree, comparator, start, startInclusive, end, endInclusive, forwards);
        return r;
    }

    public static <V> V find(Object[] node, Comparator<V> comparator, V find)
    {
        while (true)
        {
            int keyEnd = getKeyEnd(node);
            int i = BTree.find(comparator, find, node, 0, keyEnd);
            if (i >= 0)
            {
                return (V) node[i];
            }
            else if (!isLeaf(node))
            {
                i = -i - 1;
                node = (Object[]) node[keyEnd + i];
            }
            else
            {
                return null;
            }
        }
    }


    // UTILITY METHODS

    // same basic semantics as Arrays.binarySearch
    static <V> int find(Comparator<V> comparator, Object key, Object[] a, final int fromIndex, final int toIndex) {
        // attempt to terminate quickly by checking the first element,
        // as many uses of this class will (probably) be updating identical sets
        if (fromIndex >= toIndex)
            return -(fromIndex + 1);

        int c = compare(comparator, key, a[fromIndex]);
        if (c <= 0)
        {
            if (c == 0)
                return fromIndex;
            else
                return -(fromIndex + 1);
        }

        int low = fromIndex + 1;
        int high = toIndex - 1;

        while (low <= high) {
            int mid = (low + high) >>> 1;
            int cmp = compare(comparator, key, a[mid]);

            if (cmp > 0)
                low = mid + 1;
            else if (cmp < 0)
                high = mid - 1;
            else
                return mid; // key found
        }
        return -(low + 1);  // key not found.
    }

    // get the upper bound we should search in for keys in the node
    static int getKeyEnd(Object[] node)
    {
        if (isLeaf(node))
            return getLeafKeyEnd(node);
        else
            return getBranchKeyEnd(node);
    }

    // get the last index that is non-null in the leaf node
    static int getLeafKeyEnd(Object[] node)
    {
        int len = node.length;
        if (len == 0)
            return 0;
        else if (node[len - 1] == null)
            return len - 1;
        else
            return len;
    }

    // return the boundary position between keys/children for the branch node
    static int getBranchKeyEnd(Object[] node)
    {
        return node.length >> 1;
    }

    // returns true if the provided node is a leaf, false if it is a branch
    static boolean isLeaf(Object[] node)
    {
        return (node.length & 1) == 0;
    }

    // Special class for making certain operations easier, so we can define a +/- Inf
    private static interface Special extends Comparable<Object> { }
    static final Special POSITIVE_INFINITY = new Special()
    {
        @Override
        public int compareTo(Object o)
        {
            return o == this ? 0 : 1;
        }
    };
    static final Special NEGATIVE_INFINITY = new Special()
    {
        @Override
        public int compareTo(Object o)
        {
            return o == this ? 0 : -1;
        }
    };

    private static final ThreadLocal<Modifier> MODIFIER = new ThreadLocal<Modifier>()
    {
        @Override
        protected Modifier initialValue()
        {
            return new Modifier();
        }
    };

    // return a sorted collection
    private static <V> Collection<V> sorted(Collection<V> collection, Comparator<V> comparator, int size)
    {
        V[] vs = collection.toArray((V[]) new Object[size]);
        Arrays.sort(vs, comparator);
        return Arrays.asList(vs);
    }

    // TODO : cheaper to check for POSITIVE/NEGATIVE infinity in callers, rather than here
    // simple static wrapper to calls to cmp.compare() which checks if either a or b are Special (i.e. represent an infinity)
    static <V> int compare(Comparator<V> cmp, Object a, Object b)
    {
        if (a instanceof Special)
            return ((Special) a).compareTo(b);
        if (b instanceof Special)
            return -((Special) b).compareTo(a);
        return cmp.compare((V) a, (V) b);
    }

}
