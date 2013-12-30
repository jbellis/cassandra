package org.apache.cassandra.utils.concurrent;

import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

public abstract class AbstractNonBlockingQueue<N extends AbstractNonBlockingQueue.Node<N>>
{

    public static class Node<N extends Node<N>>
    {
        protected volatile N next;
        protected static final AtomicReferenceFieldUpdater<Node, Node> nextUpdater = AtomicReferenceFieldUpdater.newUpdater(Node.class, Node.class, "next");
        protected void reuse()
        {
            next = null;
        }
    }

    protected volatile N head, tail;
    protected static final AtomicReferenceFieldUpdater<AbstractNonBlockingQueue, Node> headUpdater = AtomicReferenceFieldUpdater.newUpdater(AbstractNonBlockingQueue.class, Node.class, "head");
    protected static final AtomicReferenceFieldUpdater<AbstractNonBlockingQueue, Node> tailUpdater = AtomicReferenceFieldUpdater.newUpdater(AbstractNonBlockingQueue.class, Node.class, "tail");

    protected AbstractNonBlockingQueue(N newList)
    {
        this.head = this.tail = newList;
    }

    protected AbstractNonBlockingQueue(N head, N tail)
    {
        this.head = head;
        this.tail = tail;
    }

    /**
     * removes the provided item from the queue iff it is currently the first item (i.e. the next to be returned)
     * @param expectedHead the item we expect to be first in the queue
     * @return true iff success
     */
    protected final boolean advanceHeadIf(N expectedHead)
    {
        assert expectedHead != null;
        // we loop only in case compareAndSet may fail spuriously, which it shouldn't
        // but the spec doesn't guarantee this
        while (true)
        {
            N head = this.head;
            N next = head.next;
            if (next == null || next != expectedHead)
                return false;
            if (headUpdater.compareAndSet(this, head, next))
            {
                if (tail == head)
                    tailUpdater.compareAndSet(this, head, next);
                return true;
            }
        }
    }

    /**
     * @return the first item in the queue, after atomically removing it
     */
    protected final N pollNode()
    {
        while (true)
        {
            N head = this.head;
            N next = head.next;
            if (next == null)
                return null;
            if (headUpdater.compareAndSet(this, head, next))
            {
                if (tail == head)
                    tailUpdater.compareAndSet(this, head, next);
                return next;
            }
        }
    }

    /**
     * The next item to be returned from the queue, or null of none.
     * @return
     */
    protected final N peekNode()
    {
        return head.next;
    }

    /**
     * @return true iff the queue is currently empty
     */
    public final boolean isEmpty()
    {
        return head.next == null;
    }

    /**
     * Finds the tail, starting from the provided tail parameter, updating
     * the global tail property if the tail we find is not the same
     *
     * @return
     */
    protected final N refreshTailNode()
    {
        while (true)
        {
            N prev = tail;
            N tail = prev;
            while (tail.next != null)
                tail = tail.next;
            // we perform a cas to make sure we never get too far behind the head pointer,
            // to avoid retaining more garbage than necessary
            if (prev == tail || tailUpdater.compareAndSet(this, prev, tail))
                return tail;
        }
    }

    /**
     * Returns the tail node of the queue, which will be on or after the value of head before the method call
     *
     * @return
     */
    protected N tailNode()
    {
        N tail = this.tail;
        while (tail.next != null)
            tail = tail.next;
        return tail;
    }

    /**
     * Add the provided item to the end of the queue
     *
     * @param append
     */
    protected final void appendNode(N append)
    {
        while (true)
        {
            N tail = refreshTailNode();
            if (Node.nextUpdater.compareAndSet(tail, null, append))
                return;
        }
    }

}
