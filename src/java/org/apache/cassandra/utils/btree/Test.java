package org.apache.cassandra.utils.btree;

import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Timer;
import com.yammer.metrics.core.TimerContext;
import com.yammer.metrics.stats.Snapshot;
import edu.stanford.ppl.concurrent.SnapTreeMap;

import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Random;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;

public class Test
{

    private static String deepToString(Object[] a)
    {
        StringBuilder b = new StringBuilder("[");
        boolean first = true;
        for (Object o : a)
        {
            if (first)
                first = false;
            else
                b.append(", ");
            if (o instanceof Object[])
                b.append(deepToString((Object[]) o));
            else
                b.append(o);
        }
        b.append("]");
        return b.toString();
    }

    private static final Timer BTREE_TIMER = Metrics.newTimer(BTree.class, "BTREE", TimeUnit.NANOSECONDS, TimeUnit.NANOSECONDS);
    private static final Timer TREE_TIMER = Metrics.newTimer(BTree.class, "TREE", TimeUnit.NANOSECONDS, TimeUnit.NANOSECONDS);

    public static void main(String[] args)
    {
        testInsertions();
    }

    private static void testInsertions()
    {
        int totalCount = 100000000;
        int testCount = 50;
        int testRange = testCount * 10;
        int batchSize = 1;
        int batchesPerTest = testCount / batchSize;
        int maximumRunLength = 100;
        int tests = totalCount / testCount;
        for (int i = 0 ; i < tests ; i++)
        {
            testInsertions(testRange, maximumRunLength, batchSize, batchesPerTest);
        }
        Snapshot snap = BTREE_TIMER.getSnapshot();
        System.out.println(String.format("btree  : %.2fns, %.2fsns, %.2fsns", snap.getMedian(), snap.get95thPercentile(), snap.get999thPercentile()));
        snap = TREE_TIMER.getSnapshot();
        System.out.println(String.format("treeset: %.2fns, %.2fsns, %.2fsns", snap.getMedian(), snap.get95thPercentile(), snap.get999thPercentile()));
    }

    private static void testInsertions(int upperBound, int maxRunLength, int averageModsPerIteration, int iterations)
    {
        SnapTreeMap<Integer, Integer> canon = new SnapTreeMap<>();
        Object[] btree = BTree.empty();
        final TreeMap<Integer, Integer> buffer = new TreeMap<>();
        final Random rnd = new Random();
        for (int i = 0 ; i < iterations ; i++)
        {
            buffer.clear();
            int mods = (averageModsPerIteration >> 1) + 1 + rnd.nextInt(averageModsPerIteration);
            while (mods > 0)
            {
                int v = rnd.nextInt(upperBound);
                int rc = Math.max(0, Math.min(mods, maxRunLength) - 1);
                int c = 1 + (rc <= 0 ? 0 : rnd.nextInt(rc));
                for (int j = 0 ; j < c ; j++)
                {
                    buffer.put(v, v);
                    v++;
                }
                mods -= c;
            }
            TimerContext ctxt;
            ctxt = TREE_TIMER.time();
            canon = canon.clone();
            canon.putAll(buffer);
            ctxt.stop();
            ctxt = BTREE_TIMER.time();
            btree = BTree.update(btree, ICMP, buffer.keySet(), true, null, null);
            ctxt.stop();
            test(BTree.<Integer>slice(btree, true), canon.keySet().iterator());
        }
    }

    private static <V> void test(Iterator<V> btree, Iterator<V> canon)
    {
        while (btree.hasNext() && canon.hasNext())
        {
            Object i = btree.next();
            Object j = canon.next();
            if (!i.equals(j))
                System.out.println("Expected " + j + ", got " + i);
        }
        while (btree.hasNext())
            System.out.println("Expected <Nil>, got " + btree.next());
        while (canon.hasNext())
            System.out.println("Expected " + canon.next() + ", got <Nil>");
    }

    private static void testIterators()
    {
        Object[] cur = BTree.empty();
        for (int i = 0 ; i < 500 ; i++)
        {
            test(cur, i);
            cur = BTree.update(cur, ICMP, Arrays.asList(i), true, null, null);
        }
    }

    static final Comparator<Integer> ICMP = new Comparator<Integer>()
    {
        @Override
        public int compare(Integer o1, Integer o2)
        {
            return Integer.compare(o1, o2);
        }
    };

    private static void test(Object[] btree, int count)
    {
        String id = String.format("[0..%d)", count);
        System.out.println("Testing " + id);
        testAscendingIterator(id, BTree.<Integer>slice(btree, true), 0, count, count);
        testDescendingIterator(id, BTree.<Integer>slice(btree, false), 0, count, count);
        id = id + " Sub";
        for (int i = -1 ; i < count ; i++)
        {
            for (int j = i ; j < count + 1 ; j++)
            {
                int size = Math.max(j, 0) - Math.max(i, 0);
                testAscendingIterator(id, BTree.<Integer>slice(btree, ICMP, i, j, true), i, j, size);
                testDescendingIterator(id, BTree.<Integer>slice(btree, ICMP, i, j, false), i, j, size);
            }
        }
    }

    private static void testAscendingIterator(String id, Iterator<Integer> iter, int first, int ub, int count)
    {
        id = String.format("Asc  %s [%d..%d)", id, first, ub);
        int i = Math.max(0, first);
        int c = 0;
        while (iter.hasNext())
        {
            test(id, i++, iter.next());
            c++;
        }
        test(id + " Count", count, c);
    }

    private static void testDescendingIterator(String id, Iterator<Integer> iter, int first, int ub, int count)
    {
        id = String.format("Desc %s [%d..%d)", id, first, ub);
        int i = ub - 1;
        int c = 0;
        while (iter.hasNext())
        {
            test(id, i--, iter.next());
            c++;
        }
        test(id + " Count", count, c);
    }

    private static void test(String test, int j, int j2)
    {
        if (j != j2)
        {
            System.out.println(String.format("%s: Expected %d, Got %d", test, j, j2));
        }
    }

}
