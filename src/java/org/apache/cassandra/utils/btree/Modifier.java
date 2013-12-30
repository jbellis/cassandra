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
 * to ModifierLevel which represents a single stack item, performing operations on the current level for each modification,
 * and moving the stack pointer / level as dictated by the result of that operation.
 * <p/>
 * This is a fairly heavy-weight object, so a ThreadLocal instance is created for making modifications to a tree
 */
final class Modifier
{
    private final ModifierLevel stack = new ModifierLevel(); // TODO rename this since we have a Stack that is unrelated?

    /**
     * Assumes @param source has been sorted, e.g. by BTree.update
     */
    public <V> Object[] update(Object[] btree, Comparator<V> comparator, Collection<V> source, ReplaceFunction<V> replaceF, Function<?, Boolean> terminateEarly)
    {
        ModifierLevel current = stack;
        current.reset(btree, POSITIVE_INFINITY);

        for (V key : source)
        {
            while (true)
            {
                if (terminateEarly != null && terminateEarly.apply(null) == Boolean.TRUE)
                {
                    clear(stack);
                    return null;
                }
                ModifierLevel next = current.update(key, comparator, replaceF);
                if (next == null)
                    break;
                current = next;
            }
        }

        // finish copying any remaining keys
        while (true)
        {
            ModifierLevel next = current.update(POSITIVE_INFINITY, comparator, replaceF);
            if (next == null)
                break;
            current = next;
        }

        Object[] r = current.toNode();
        clear(current);
        return r;
    }

    public <V> Object[] build(Collection<V> source, int size)
    {
        ModifierLevel current = stack;
        do
        {
            current.reset(EMPTY_BRANCH, POSITIVE_INFINITY);
            current = current.ensureChild();
        } while ((size >>= FAN_SHIFT) > 0);

        current.reset(EMPTY_LEAF, POSITIVE_INFINITY);
        for (V key : source)
            current.addNewKey(key, null);

        current = current.ascendToRoot();

        Object[] r = current.toNode();
        clear(current);
        return r;
    }

    private static void clear(ModifierLevel base)
    {
        ModifierLevel current = base;
        while (current != null)
        {
            current.clear();
            current = current.child;
        }
    }
}