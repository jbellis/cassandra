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
class Stack
{
    static enum Find
    {
        CEIL, FLOOR, HIGHER, LOWER
    }

    static Stack newStack()
    {
        // try to encourage stack allocation - probably misguided/unnecessary. but no harm
        Object[][] stack = new Object[MAX_DEPTH][];
        byte[] index = new byte[MAX_DEPTH];
        return new Stack(stack, index);
    }

    final Object[][] stack;
    final byte[] index;
    byte depth;

    Stack(Object[][] stack, byte[] index)
    {
        this.stack = stack;
        this.index = index;
    }

    /**
     * Find the provided key in the tree rooted at node, and store the root to it in the stack
     *
     * @param node       the tree to search in
     * @param comparator the comparator defining the order on the tree
     * @param find       the key to search for
     * @param mode       the type of search to perform
     * @param forwards   if the stack should be setup for forward or backward iteration
     * @param <V>
     */
    <V> void find(Object[] node, Comparator<V> comparator, Object find, Find mode, boolean forwards)
    {
        // TODO : should not require parameter 'forwards' - consider modifying index to represent both
        // child and key position, as opposed to just key position (which necessitates a different value depending
        // on which direction you're moving in. Prerequisite for making Stack public and using to implement general
        // search

        depth = 0;
        while (true)
        {
            stack[depth] = node;
            int keyEnd = getKeyEnd(node);
            int i = BTree.find(comparator, find, node, 0, keyEnd);
            if (i >= 0)
            {
                index[depth] = (byte) i;
                switch (mode)
                {
                    case HIGHER:
                        successor(node, i);
                        break;
                    case LOWER:
                        predecessor(node, i);
                }
                return;
            }
            else if (!isLeaf(node))
            {
                i = -i - 1;
                node = (Object[]) node[keyEnd + i];
                index[depth] = (byte) (forwards ? i - 1 : i);
                ++depth;
            }
            else
            {
                i = -i - 1;
                switch (mode)
                {
                    case FLOOR:
                    case LOWER:
                        i--;
                        break;
                }
                if (i < 0)
                {
                    index[depth] = 0;
                    predecessor(node, 0);
                }
                else if (i >= keyEnd)
                {
                    index[depth] = (byte) (keyEnd - 1);
                    successor(node, keyEnd - 1);
                }
                else
                {
                    index[depth] = (byte) i;
                }
                return;
            }
        }
    }

    // move to the next key in the tree
    void successor(Object[] cur, int curi)
    {
        byte d = depth;
        if (!isLeaf(cur))
        {
            // if we're on a key in a leaf, we MUST have a descendant either side of us
            // so we always go down
            while (true)
            {
                d++;
                cur = (stack[d] = (Object[]) cur[getBranchKeyEnd(cur) + curi + 1]);
                if (isLeaf(cur))
                {
                    index[d] = 0;
                    depth = d;
                    return;
                }
                curi = index[d] = -1;
            }
        }
        else
        {
            // go up until we reach something we're not at the end of
            curi += 1;
            int curKeyEnd = getLeafKeyEnd(cur);
            if (curi < curKeyEnd)
            {
                index[d] = (byte) curi;
                return;
            }
            do
            {
                if (d == 0)
                {
                    index[d] = (byte) curKeyEnd;
                    depth = d;
                    return;
                }
                d--;
                curi = index[d] + 1;
                curKeyEnd = getKeyEnd(stack[d]);
                if (curi < curKeyEnd)
                {
                    index[d] = (byte) curi;
                    depth = d;
                    return;
                }
            } while (true);
        }
    }

    // move to the previous key in the tree
    void predecessor(Object[] cur, int curi)
    {
        byte d = depth;
        if (!isLeaf(cur))
        {
            int curKeyEnd = getBranchKeyEnd(cur);
            while (true)
            {
                d++;
                cur = (stack[d] = (Object[]) cur[curKeyEnd + curi]);
                if (isLeaf(cur))
                {
                    index[d] = (byte) (getLeafKeyEnd(cur) - 1);
                    depth = d;
                    return;
                }
                curKeyEnd = getBranchKeyEnd(cur);
                curi = index[d] = (byte) curKeyEnd;
            }
        }
        else
        {
            // go up until we reach something we're not at the end of!
            curi -= 1;
            if (curi >= 0)
            {
                index[d] = (byte) curi;
                return;
            }
            do
            {
                if (d == 0)
                {
                    index[d] = -1;
                    depth = d;
                    return;
                }
                d--;
                curi = index[d] - 1;
                if (curi >= 0)
                {
                    index[d] = (byte) curi;
                    depth = d;
                    return;
                }
            } while (true);
        }
    }

    int compareTo(Stack that, boolean forwards)
    {
        int d = Math.min(this.depth, that.depth);
        for (int i = 0; i <= d; i++)
        {
            int c = this.index[i] - that.index[i];
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

