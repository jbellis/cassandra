package org.apache.cassandra.utils.concurrent;

import java.util.Iterator;

/**
 * A poll-only view on a {@link NonBlockingQueue}
 *
 * Note that polling/advancing this view will not affect the position of the queue it is a view of, however successful
 * Iterator removals will *generally* (though are not guaranteed to) be reflected in the original queue, and all other views.
 */
public class NonBlockingQueueView<V> extends AbstractNonBlockingQueue<NonBlockingQueueView.Node<V>> implements Iterable<V>
{

    static final class Node<V> extends AbstractNonBlockingQueue.Node<Node<V>>
    {
        /**
         * a flag that indicates we should NOT attempt to remove the node from the list
         * as it may be used in bookkeeping for a Snap of the list
         */
        volatile boolean locked;
        final V value;

        public Node(V value)
        {
            this.value = value;
        }
    }

    protected NonBlockingQueueView(Node<V> head)
    {
        this(head, head);
    }

    protected NonBlockingQueueView(Node<V> head, Node<V> tail)
    {
        super(head, tail);
    }

    /**
     * removes the provided item from the queue iff it is currently the first item (i.e. the next to be returned)
     * by object identity (==)
     * @param expectedHead the item we expect to be first in the queue
     * @return true iff success
     */
    public boolean advanceHeadIf(V expectedHead)
    {
        assert expectedHead != null;
        Node<V> next = head.next;
        if (next != null && next.value == expectedHead)
            return advanceHeadIf(next);
        return false;
    }

    /**
     * @return the first item in the queue, after atomically removing it
     */
    public V poll()
    {
        Node<V> poll = pollNode();
        return poll == null ? null : poll.value;
    }

    /**
     * The next item to be returned from the queue, or null of none.
     * @return
     */
    public V peek()
    {
        Node<V> peek = peekNode();
        return peek == null ? null : peek.value;
    }

    /**
     * A view over all items inserted or polled after it was created. Equivalent to view().iterator()
     * @return
     */
    public Iterator<V> iterator()
    {
        return new SliceIterator(head, null);
    }

    /**
     * Returns an Iterable over all items either inserted or polled after it was created, excluding some subset
     * of those that are deleted through Iterator.remove().
     * Whilst this object is reachable, all items polled after it was created will continue to also
     * be retained.
     * @return
     */
    public NonBlockingQueueView<V> view()
    {
        Node<V> head = this.head;
        Node<V> tail = this.tailNode();
        return new NonBlockingQueueView<>(head, tail);
    }

    /**
     * Returns an Iterable over all items currently in the queue, excluding some subset of those that are deleted
     * using Iterator.remove().
     *
     * @return
     */
    public Snap<V> snap()
    {
        Node<V> head = this.head;
        /**
         * We ensure safety of iteration by locking the tail at the point of snap to prevent it being deleted,
         * so that we are guaranteed to always reach it from any predecessor node.
         * We must select the tail AFTER head to ensure it can be reached from head.
         */
        Node<V> tail = snapTail();
        return new SnapImpl(head, tail);
    }

    /**
     * Finds the last item in the list, locks it from being deleted, and returns it
     * if it is still the last item. As we never remove the last item, we know
     * it was still present in the list when we locked it, and that as such the locking
     * was successful (i.e. synchronized with any remove())
     */
    private Node<V> snapTail()
    {
        Node<V> tail = this.tail;
        while (true)
        {
            while (tail.next != null)
                tail = tail.next;
            tail.locked = true;
            if (tail.next == null)
                return tail;
        }
    }

    /**
     * Makes a very poor (but simple and quick) effort to remove the item from the queue; the precondition is that
     * pred was at some point the direct predecessor of remove.
     * This method works on the assumption that the queue is only ever appended to, and that the only modifications
     * to the middle of the queue are these poor delete attempts; given this second requirement and that individual
     * deletes always leave non-deleted items reachable, when deletes race we can only lose deletes, by restoring a pointer
     * to an already deleted node. Non-deleted items will always be reachable.
     * In order to preserve safety of snapshot iterators, when creating a snapshot we lock the tail pointer so that it may
     * also never be deleted; it would be possible to optimise this behaviour to allow more deletes, and also to make
     * deletes less racey (or completely safe), but neither are necessary as this algorithm is plenty sufficient for
     * the current purpose, and comparatively simple to understand to boot.
     *
     * @param pred
     * @param remove
     */
    private void remove(Node<V> pred, Node<V> remove)
    {
        Node<V> next = remove.next;
        if (next == null)
            return;
        // we check locked status AFTER checking next for null
        // to ensure we are synchronized with snapTail()
        if (remove.locked)
            return;
        pred.next = next;
        if (head == remove)
            advanceHeadIf(remove);
    }

    /**
     * Represents a consistent sub-queue, whose start and end are unaffected by poll() and append() operations on the
     * queue it is formed from, but will reflect any 'successful' Iterator removals, as defined elsewhere.
     * @param <V>
     */
    public static interface Snap<V> extends Iterable<V>
    {
        /**
         * @return the last item in this snap, or null if the snap is empty
         */
        public V tail();

        /**
         * @return a view of the entire queue, starting at the same position as this Snap
         */
        public NonBlockingQueueView<V> view();
    }

    private class SnapImpl implements Snap<V>
    {
        final Node<V> head;
        final Node<V> tail;
        public SnapImpl(Node<V> head, Node<V> tail)
        {
            this.head = head;
            this.tail = tail;
        }

        @Override
        public Iterator<V> iterator()
        {
            return new SliceIterator(head, tail);
        }

        @Override
        public V tail()
        {
            return tail == head ? null : tail.value;
        }

        public NonBlockingQueueView<V> view()
        {
            return new NonBlockingQueueView<>(head, tail);
        }

    }

    private class SliceIterator implements Iterator<V>
    {

        Node<V> pred;
        Node<V> cursor;
        final Node<V> end;
        public SliceIterator(Node<V> cursor, Node<V> end)
        {
            this.cursor = cursor;
            this.end = end;
        }

        @Override
        public boolean hasNext()
        {
            Node<V> cursor = this.cursor;
            return cursor != end && cursor.next != null;
        }

        @Override
        public V next()
        {
            pred = cursor;
            Node<V> next = cursor.next;
            cursor = next;
            return next.value;
        }

        @Override
        public void remove()
        {
            NonBlockingQueueView.this.remove(pred, cursor);
        }

    }

}
