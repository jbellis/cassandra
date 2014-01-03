package org.apache.cassandra.utils.btree;

import java.util.Collection;
import java.util.Comparator;

import com.google.common.base.Function;

import static org.apache.cassandra.utils.btree.BTree.EMPTY_BRANCH;
import static org.apache.cassandra.utils.btree.BTree.EMPTY_LEAF;
import static org.apache.cassandra.utils.btree.BTree.FAN_SHIFT;
import static org.apache.cassandra.utils.btree.BTree.POSITIVE_INFINITY;

/**
 * A class for constructing a new BTree, either from an existing one and some set of modifications
 * or a new tree from a sorted collection of items. It works largely as a Stack of in progress modifications, delegating
 * to NodeBuilder which represents a single node-in-progress, performing operations on the current level for each modification,
 * and moving the stack pointer / level as dictated by the result of that operation.
 * <p/>
 * This is a fairly heavy-weight object, so a ThreadLocal instance is created for making modifications to a tree
 */
final class Builder
{
    private final NodeBuilder rootBuilder = new NodeBuilder();

    /**
     * Assumes @param source has been sorted, e.g. by BTree.update
     */
    public <V> Object[] update(Object[] btree, Comparator<V> comparator, Collection<V> source, ReplaceFunction<V> replaceF, Function<?, Boolean> terminateEarly)
    {
        NodeBuilder current = rootBuilder;
        current.reset(btree, POSITIVE_INFINITY);

        for (V key : source)
        {
            while (true)
            {
                if (terminateEarly != null && terminateEarly.apply(null) == Boolean.TRUE)
                {
                    rootBuilder.clear();
                    return null;
                }
                NodeBuilder next = current.update(key, comparator, replaceF);
                if (next == null)
                    break;
                current = next;
            }
        }

        // finish copying any remaining keys from the original btree
        while (true)
        {
            NodeBuilder next = current.update(POSITIVE_INFINITY, comparator, replaceF);
            if (next == null)
                break;
            current = next;
        }

        Object[] r = current.toNode();
        current.clear();
        return r;
    }

    public <V> Object[] build(Collection<V> source, int size)
    {
        NodeBuilder current = rootBuilder;
        // we descend only to avoid wasting memory; in update() we will often descend into existing trees
        // so here we want to descend also, so we don't have lg max(N) depth in both directions
        while ((size >>= FAN_SHIFT) > 0)
            current = current.ensureChild();

        current.reset(EMPTY_LEAF, POSITIVE_INFINITY);
        for (V key : source)
            current.addNewKey(key, null);

        current = current.ascendToRoot();

        Object[] r = current.toNode();
        current.clear();
        return r;
    }
}