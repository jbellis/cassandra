package org.apache.cassandra.utils.concurrent;

/**
 * A minimal queue API on which all operations are non-blocking and atomic wrt each other (except Iterator removal).
 * This queue is designed with predictable performance and ease of understanding in mind, NOT absolute performance,
 * though it should perform very well still.
 *
 * The queue is effectively two pointers into a singly linked list which can only safely be appended to;
 * poll() only advances the head pointer, with the prefix left for GC to deal with. As a result we can easily
 * create persistent views and snapshots of the list.
 *
 * To help certain use cases, Iterator removal is supported from the middle of the list, but it is only a fairly poor
 * effort attempt: it is not guaranteed to succeed, and if it is interleaved with competing removes or Snap creations
 * it may be ignored, and may even restore previously successfully removed items. It will never remove the tail item.
 * It should not be used in situations where removal is anything more than a convenience, or where consistency
 * of removal is required.
 *
 * Note that the queue will always keep a reference to the last item polled
 *
 * @param <V>
 */
public class NonBlockingQueue<V> extends NonBlockingQueueView<V> implements Iterable<V>
{

    public NonBlockingQueue()
    {
        super(new Node<V>(null));
    }

    /**
     * Add the provided item to the end of the queue
     *
     * @param append
     */
    public void append(V append)
    {
        appendNode(new Node<V>(append));
    }

    /**
     * Add <code>append</code> to the end of the queue iff the end of the queue is currently
     * <code>expectedTail</code>. Note that this works even if the queue is now empty but the last item
     * prior to the queue being empty was <code>expectedTail</code>.
     *
     * @param expectedTail the last item we expect to be in the queue, or the last item returned if empty
     * @param append the item to add
     * @return true iff success
     */
    public boolean appendIfTail(V expectedTail, V append)
    {
        Node<V> tail = refreshTailNode();
        if (expectedTail != tail.value)
            return false;
        return Node.nextUpdater.compareAndSet(tail, null, new Node<V>(append));
    }

    /**
     * Returns the tail of the queue. NOTE that this succeeds even if the queue is empty,
     * returning the most recently polled item in this case. If the queue has always been empty, null
     * is returned.
     * @return
     */
    public V tail()
    {
        return tailNode().value;
    }

    /**
     * Advances the queue past tail. Does not clean up any dangling pointers.
     */
    public void clear()
    {
        head = refreshTailNode();
    }

}
