package org.apache.cassandra.utils.btree;

import java.util.Comparator;

import static org.apache.cassandra.utils.btree.BTree.MAX_DEPTH;
import static org.apache.cassandra.utils.btree.BTree.getBranchKeyEnd;
import static org.apache.cassandra.utils.btree.BTree.getKeyEnd;
import static org.apache.cassandra.utils.btree.BTree.getLeafKeyEnd;
import static org.apache.cassandra.utils.btree.BTree.isLeaf;

/**
 * An internal class for searching and iterating through a tree
 */
class Path
{
    // operations corresponding to the ones in NavigableSet
    static enum Op
    {
        CEIL,   // the least element greater than or equal to the given element
        FLOOR,  // the greatest element less than or equal to the given element
        HIGHER, // the least element strictly greater than the given element
        LOWER   // the greatest element strictly less than the given element
    }

    static Path newPath()
    {
        // try to encourage stack allocation - probably misguided/unnecessary. but no harm
        Object[][] path = new Object[MAX_DEPTH][];
        byte[] index = new byte[MAX_DEPTH];
        return new Path(path, index);
    }

    // the path to the searched-for key
    final Object[][] path;
    // the index within the node of our path at a given depth
    final byte[] indexes;
    // current depth
    byte depth;

    Path(Object[][] path, byte[] indexes)
    {
        this.path = path;
        this.indexes = indexes;
    }

    /**
     * Find the provided key in the tree rooted at node, and store the root to it in the path
     *
     * @param node       the tree to search in
     * @param comparator the comparator defining the order on the tree
     * @param target     the key to search for
     * @param mode       the type of search to perform
     * @param forwards   if the path should be setup for forward or backward iteration
     * @param <V>
     */
    <V> void find(Object[] node, Comparator<V> comparator, Object target, Op mode, boolean forwards)
    {
        // TODO : should not require parameter 'forwards' - consider modifying index to represent both
        // child and key position, as opposed to just key position (which necessitates a different value depending
        // on which direction you're moving in. Prerequisite for making Path public and using to implement general
        // search

        depth = -1;
        while (true)
        {
            int keyEnd = getKeyEnd(node);

            // search for the target in the current node
            int i = BTree.find(comparator, target, node, 0, keyEnd);
            if (i >= 0)
            {
                push(node, i);
                // transform exclusive bounds into the correct index by moving back or forwards one
                switch (mode)
                {
                    case HIGHER:
                        successor();
                        break;
                    case LOWER:
                        predecessor();
                }
                return;
            }

            // traverse into the appropriate child
            if (!isLeaf(node))
            {
                i = -i - 1;
                push(node, forwards ? i - 1 : i);
                node = (Object[]) node[keyEnd + i];
                continue;
            }

            // bottom of the tree and still not found.  pick the right index to satisfy Op
            i = -i - 1;
            switch (mode)
            {
                case FLOOR:
                case LOWER:
                    i--;
            }

            if (i < 0)
            {
                push(node, 0);
                predecessor();
            }
            else if (i >= keyEnd)
            {
                push(node, keyEnd - 1);
                successor();
            }
            else
            {
                push(node, i);
            }

            return;
        }
    }

    private boolean isRoot()
    {
        return depth == 0;
    }

    private void pop()
    {
        depth--;
    }

    Object[] currentNode()
    {
        return path[depth];
    }

    int currentIndex()
    {
        return indexes[depth];
    }

    private void push(Object[] node, int index)
    {
        path[++depth] = node;
        indexes[depth] = (byte) index;
    }

    void setIndex(int index)
    {
        indexes[depth] = (byte) index;
    }

    // move to the next key in the tree
    void successor()
    {
        Object[] node = currentNode();
        int i = currentIndex();

        if (!isLeaf(node))
        {
            // if we're on a key in a leaf, we MUST have a descendant either side of us
            // so we always go down
            node = (Object[]) node[getBranchKeyEnd(node) + i + 1];
            while (!isLeaf(node))
            {
                push(node, -1);
                node = (Object[]) node[getBranchKeyEnd(node)];
            }
            push(node, 0);
            return;
        }

        i += 1;
        if (i < getLeafKeyEnd(node))
        {
            setIndex(i);
            return;
        }

        // go up until we reach something we're not at the end of
        while (true)
        {
            if (isRoot())
            {
                setIndex(getKeyEnd(node));
                return;
            }
            pop();
            i = currentIndex() + 1;
            node = currentNode();
            if (i < getKeyEnd(node))
            {
                setIndex(i);
                return;
            }
        }
    }

    // move to the previous key in the tree
    void predecessor()
    {
        Object[] node = currentNode();
        int i = currentIndex();

        if (!isLeaf(node))
        {
            node = (Object[]) node[getBranchKeyEnd(node) + i];
            while (!isLeaf(node))
            {
                i = getBranchKeyEnd(node);
                push(node, i);
                node = (Object[]) node[i << 1];
            }
            push(node, getLeafKeyEnd(node) - 1);
            return;
        }

        // go up until we reach something we're not at the end of!
        i -= 1;
        if (i >= 0)
        {
            setIndex(i);
            return;
        }
        while (true)
        {
            if (isRoot())
            {
                setIndex(-1);
                return;
            }
            pop();
            i = currentIndex() - 1;
            if (i >= 0)
            {
                setIndex(i);
                return;
            }
        }
    }

    Object currentKey()
    {
        return currentNode()[currentIndex()];
    }

    int compareTo(Path that, boolean forwards)
    {
        int d = Math.min(this.depth, that.depth);
        for (int i = 0; i <= d; i++)
        {
            int c = this.indexes[i] - that.indexes[i];
            if (c != 0)
                return c;
        }
        // identical indices up to depth, so if somebody is lower depth they are on a later item if iterating forwards
        // and an earlier item if iterating backwards, as the node at max common depth must be a branch if they are
        // different depths, and branches that are currently descended into lag the child index they are in when iterating forwards,
        // i.e. if they are in child 0 they record an index of -1 forwards, or 0 when backwards
        d = this.depth - that.depth;
        return forwards ? d : -d;
    }
}

