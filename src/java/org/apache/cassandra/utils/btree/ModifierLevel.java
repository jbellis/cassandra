package org.apache.cassandra.utils.btree;

import java.util.Arrays;
import java.util.Comparator;

import static org.apache.cassandra.utils.btree.BTree.EMPTY_BRANCH;
import static org.apache.cassandra.utils.btree.BTree.FAN_FACTOR;
import static org.apache.cassandra.utils.btree.BTree.POSITIVE_INFINITY;
import static org.apache.cassandra.utils.btree.BTree.compare;
import static org.apache.cassandra.utils.btree.BTree.find;
import static org.apache.cassandra.utils.btree.BTree.getKeyEnd;
import static org.apache.cassandra.utils.btree.BTree.isLeaf;

/**
 * A class representing a level / stack item of in progress modifications to a BTree. This is a fairly heavy-weight
 * object, so a ThreadLocal instance is created for making modifications to a set
 */
final class ModifierLevel
{
    // parent stack
    ModifierLevel parent, child;

    // buffer for building new nodes
    Object[] buildKeys = new Object[1 + (FAN_FACTOR << 1)];  // buffers keys for branches and leaves
    Object[] buildChildren = new Object[2 + (FAN_FACTOR << 1)]; // buffers children for branches only
    int buildKeyPosition;
    int buildChildPosition;
    int maxBuildKeyPosition;
    int maxBuildChildPosition;

    // copying from
    Object[] copyf;
    int copyfKeyPosition;
    int copyfChildPosition;
    int copyfKeyEnd;

    // upper bound of range owned by this level
    Object upperBound;

    // ensure we aren't referencing any garbage
    boolean clear()
    {
        if (upperBound == null)
            return false;
        reset(null, null);
        Arrays.fill(buildKeys, 0, maxBuildKeyPosition, null);
        Arrays.fill(buildChildren, 0, maxBuildChildPosition, null);
        maxBuildChildPosition = maxBuildKeyPosition = 0;
        return true;
    }

    // reset counters/setup to copy from provided node
    void reset(Object[] cpf, Object upperBound)
    {
        this.copyf = cpf;
        this.upperBound = upperBound;
        maxBuildKeyPosition = Math.max(maxBuildKeyPosition, buildKeyPosition);
        maxBuildChildPosition = Math.max(maxBuildChildPosition, buildChildPosition);
        buildKeyPosition = 0;
        buildChildPosition = 0;
        copyfKeyPosition = 0;
        copyfChildPosition = 0;
        if (cpf != null)
            copyfKeyEnd = getKeyEnd(cpf);
    }

    /**
     * Inserts or replaces the provided key, copying all not-yet-visited keys prior to it into the builder
     *
     * @param key key we are inserting/replacing
     * @return
     */
    <V> ModifierLevel update(Object key, Comparator<V> comparator, ReplaceFunction<V> replaceF)
    {
        // true iff we found the exact key in this node
        boolean found = false;
        // true iff this node (or a child) should contain the key
        boolean owns = true;
        // find insert pos
        int pos = find(comparator, (V) key, copyf, copyfKeyPosition, copyfKeyEnd);
        if (pos < 0)
        {
            pos = -pos - 1;
            if (pos == copyfKeyEnd && compare(comparator, upperBound, key) <= 0)
                owns = false;
        }
        else
            found = true;

        boolean isLeaf = isLeaf(copyf);

        if (isLeaf)
        {
            // copy any keys up to prior to the found index
            copyKeys(pos);

            if (owns)
            {
                if (found)
                    replaceNextKey(key, replaceF);
                else
                    addNewKey(key, replaceF);

                // done, so return null
                return null;
            }

            // if we don't own it, all we need to do is ensure we've copied everything in this node
            // and the next part will deal with ascending; since not owning means pos >= keyEnd
            // we have already dealt with that

        }
        else
        {

            if (found)
            {
                copyKeys(pos);
                replaceNextKey(key, replaceF);
                copyChildren(pos + 1);
                return null;
            }
            else if (owns)
            {

                copyKeys(pos);
                copyChildren(pos);

                // belongs to the range owned by this node, but not equal to any key in the node
                // so descend into the owning child
                Object newUpperBound;
                if (pos < copyfKeyEnd)
                    newUpperBound = copyf[pos];
                else
                    newUpperBound = upperBound;
                Object[] descendInto = (Object[]) copyf[copyfKeyEnd + pos];
                ensureChild().reset(descendInto, newUpperBound);
                return child;

            } else {

                // ensure we've copied all keys and children
                copyKeys(copyfKeyEnd);
                copyChildren(copyfKeyEnd + 1);

            }

        }

        if (key == POSITIVE_INFINITY && buildChildPosition >= 1 && buildChildPosition <= 2)
            return null;

        return ascend(isLeaf);
    }


    // UTILITY METHODS FOR IMPLEMENTATION OF UPDATE/BUILD/DELETE


    // ascend to the root node, finishing up work as we go; useful for building where we work only on the newest
    // child node, which may construct many spill-over parents as it goes
    ModifierLevel ascendToRoot()
    {
        boolean isLeaf = isLeaf(copyf);
        // <= 2 check is enough if we have FAN_FACTOR >= 8, but if FAN_FACTOR is 4 this could terminate on
        // branches that are not the root, so must check the parent is not initialised. VM should optimise this
        // check away when FAN_FACTOR >= 8.
        if (!isLeaf && buildChildPosition <= 2 && (FAN_FACTOR >= 8 || parent == null || parent.upperBound == null))
            return this;
        return ascend(isLeaf).ascendToRoot();
    }

    // finish up this level and pass any constructed children up to our parent, ensuring a parent exists
    ModifierLevel ascend(boolean isLeaf)
    {
        ensureParent();
        // we don't own it, so we're ascending, so update and return our parent
        if (buildKeyPosition > FAN_FACTOR)
        {
            int mid = buildKeyPosition >> 1;
            parent.addExtraChild(
                    buildFromRange(0, mid, isLeaf),
                    buildKeys[mid]
            );
            parent.finishChild(
                    buildFromRange(mid + 1, buildKeyPosition - (mid + 1), isLeaf)
            );
        }
        else
        {
            parent.finishChild(
                    buildFromRange(0, buildKeyPosition, isLeaf)
            );
        }
        return parent;
    }

    // copy keys from copyf to the builder, up to the provided index in copyf (exclusive)
    void copyKeys(int upToKeyPosition)
    {
        int cpfPos = copyfKeyPosition;
        if (cpfPos >= upToKeyPosition)
            return;
        copyfKeyPosition = upToKeyPosition;
        int len = upToKeyPosition - cpfPos;
        if (len > FAN_FACTOR)
            throw new IllegalStateException(upToKeyPosition + "," + cpfPos);
        ensureRoom(buildKeyPosition + len);
        System.arraycopy(copyf, cpfPos, buildKeys, buildKeyPosition, len);
        buildKeyPosition += len;
    }

    // skips the next key in copyf, and puts the provided key in the builder instead
    <V> void replaceNextKey(Object with, ReplaceFunction<V> replaceF)
    {
        ensureRoom(buildKeyPosition + 1);
        if (replaceF != null)
            with = replaceF.apply((V) copyf[copyfKeyPosition], (V) with);
        buildKeys[buildKeyPosition++] = with;
        copyfKeyPosition++;
    }

    // puts the provided key in the builder, with no impact on treatment of data from copyf
    <V> void addNewKey(Object key, ReplaceFunction<V> replaceF)
    {
        if (replaceF != null)
            key = replaceF.apply(null, (V) key);
        ensureRoom(buildKeyPosition + 1);
        buildKeys[buildKeyPosition++] = key;
    }

    // copies children from copyf to the builder, up to the provided index in copyf (exclusive)
    void copyChildren(int upToChildPosition)
    {
        // note ensureRoom isn't called here, as we should always be at/behind key additions
        int cpfPos = copyfChildPosition;
        if (cpfPos >= upToChildPosition)
            return;
        copyfChildPosition = upToChildPosition;
        int len = upToChildPosition - cpfPos;
        System.arraycopy(copyf, copyfKeyEnd + cpfPos, buildChildren, buildChildPosition, len);
        buildChildPosition += len;
    }

    // adds a new and unexpected child to the builder - called by children that overflow
    void addExtraChild(Object[] child, Object upperBound)
    {
        ensureRoom(buildKeyPosition + 1);
        buildKeys[buildKeyPosition++] = upperBound;
        buildChildren[buildChildPosition++] = child;
    }

    // adds a replacement expected child to the builder - called by children prior to ascending
    void finishChild(Object[] child)
    {
        buildChildren[buildChildPosition++] = child;
        copyfChildPosition++;
    }

    // checks if we can add the requested keys+children to the builder, and if not we spill-over into our parent
    void ensureRoom(int nextBuildKeyPosition)
    {
        if (nextBuildKeyPosition > FAN_FACTOR << 1)
        {
            // flush even number of items so we don't waste leaf space repeatedly
            Object[] flushUp = buildFromRange(0, FAN_FACTOR, isLeaf(copyf));
            ensureParent().addExtraChild(flushUp, buildKeys[FAN_FACTOR]);
            int size = FAN_FACTOR + 1;
            if (size > buildKeyPosition)
                throw new IllegalStateException(buildKeyPosition + "," + nextBuildKeyPosition);
            System.arraycopy(buildKeys, size, buildKeys, 0, buildKeyPosition - size);
            buildKeyPosition -= size;
            maxBuildKeyPosition = buildKeys.length;
            if (buildChildPosition > 0)
            {
                System.arraycopy(buildChildren, size, buildChildren, 0, buildChildPosition - size);
                buildChildPosition -= size;
                maxBuildChildPosition = buildChildren.length;
            }
        }
    }

    // builds a node from the requested builder range
    Object[] buildFromRange(int offset, int keyLength, boolean isLeaf)
    {
        Object[] a;
        if (isLeaf)
        {
            a = new Object[keyLength + (keyLength & 1)];
            System.arraycopy(buildKeys, offset, a, 0, keyLength);
        }
        else
        {
            a = new Object[1 + (keyLength << 1)];
            System.arraycopy(buildKeys, offset, a, 0, keyLength);
            System.arraycopy(buildChildren, offset, a, keyLength, keyLength + 1);
        }
        return a;
    }

    // builds a new root BTree node - must be called on root of operation
    Object[] toNode()
    {
        switch (buildChildPosition)
        {
            case 1:
                return (Object[]) buildChildren[0];
            case 2:
                return new Object[] { buildKeys[0], buildChildren[0], buildChildren[1] };
            default:
                throw new IllegalStateException();
        }

    }

    // checks if there is an initialised parent, and if not creates/initialises one and returns it.
    // different to ensureChild, as we initialise here instead of caller, as parents in general should
    // already be initialised, and only aren't in the case where we are overflowing the original root node
    ModifierLevel ensureParent()
    {
        if (parent == null)
        {
            parent = new ModifierLevel();
            parent.child = this;
        }
        if (parent.upperBound == null)
            parent.reset(EMPTY_BRANCH, upperBound);
        return parent;
    }

    // ensures a child level exists and returns it
    ModifierLevel ensureChild()
    {
        if (child == null)
        {
            child = new ModifierLevel();
            child.parent = this;
        }
        return child;
    }

}

