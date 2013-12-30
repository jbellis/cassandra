package org.apache.cassandra.utils.memory;

import org.apache.cassandra.utils.concurrent.OpOrdering;
import org.apache.cassandra.utils.concurrent.WaitQueue;

class PoolCleaner<P extends Pool> implements Runnable
{

    final P pool;

    /**
     * should ensure that at least some memory has been reclaimed after completion. should not return if clean is
     * still in progress.
     */
    final Runnable clean;
    final WaitQueue wait = new WaitQueue();

    PoolCleaner(P pool, Runnable clean)
    {
        this.pool = pool;
        this.clean = clean;
    }

    boolean needsCleaning()
    {
        return pool.offHeap.needsCleaning() || pool.onHeap.needsCleaning();
    }

    // should ONLY be called when we really think it already needs cleaning
    void trigger()
    {
        wait.signal();
    }

    void clean()
    {
        clean.run();
    }

    @Override
    public void run()
    {
        while (true)
        {
            while (!needsCleaning())
            {
                final WaitQueue.Signal signal = wait.register();
                if (!needsCleaning())
                    signal.awaitUninterruptibly();
                else
                    signal.cancel();
            }
            clean();
        }
    }

    // if can do some cleaning out-of-band, do it
    void forceClean()
    {
    }

}
