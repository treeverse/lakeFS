package io.lakefs.utils;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.After;
import org.junit.Assert;
import org.junit.Test;
import org.hamcrest.core.IsInstanceOf;

public class MatcherTest {
    private static final int SIZE = 161122;
    private static final int STOP = SIZE - 123;

    static class AnException extends Exception {
        AnException() {
            super("You know, for tests!");
        }
    }

    static protected class World extends ArrayList<List<Integer>> {
        public World(int size) {
            super(size);
            for (int i = 0; i < size; i++) {
                List a = new ArrayList<Integer>();
                a.add(i);
                this.add(a);
            }
        }

        public void merge(int from, int to) {
            Assert.assertNotNull(this.get(from));
            Assert.assertNotNull(this.get(to));
            this.get(to).addAll(this.get(from));
            this.set(from, null);
        }

        public void verify(int expectedLast) {
            int last = -1;
            for (int i = 0; i < this.size(); i++) {
                if (this.get(i) != null) {
                    Assert.assertEquals(String.format("%d: Only one non-empty slot at end", i), last, -1);
                    last = i;
                }
            }
            Assert.assertEquals(last, expectedLast);
            Assert.assertEquals(String.format("Wrong number of elements in %s", this.get(last).toString()),
                                this.get(last).size(), this.size()); 
        }
    }

    @Test
    public void oneThread() throws Matcher.FailedException, InterruptedException {
        World w = new World(SIZE);
        Matcher m = new Matcher(SIZE);

        int count;
        for (count = 0; ; count++) {
            Matcher.MergeData md = m.take();
            if (md == null) {
                break;
            }
            w.merge(md.from, md.to);
            md.onDone.run();
        }
        Assert.assertEquals(count+1, SIZE);
        int last = m.waitForEnd();
        w.verify(last);
    }

    @Test
    public void oneThreadFailure() throws Matcher.FailedException, InterruptedException {
        World w = new World(SIZE);
        Matcher m = new Matcher(SIZE);

        int count;
        for (count = 0; ; count++) {
            Matcher.MergeData md = m.take();
            if (md == null) {
                break;
            }
            w.merge(md.from, md.to);
            if (count == STOP) {
                md.onFail.accept(new AnException());
            } else {
                md.onDone.run();
            }
        }
        Assert.assertEquals("Stopped on the next iteration after failing", count, STOP + 1);
        try {
            int last = m.waitForEnd();
            Assert.fail(String.format("Expected failure but got last == %d", last));
        } catch (Matcher.FailedException e) {
            Assert.assertThat(e.getCause(), new IsInstanceOf(AnException.class));
        }
    }

    @Test
    public void manyThreads() throws Matcher.FailedException, InterruptedException {
        final int NUM_THREADS = 13;
        World w = new World(SIZE);
        Matcher m = new Matcher(SIZE);
        Thread[] thread = new Thread[NUM_THREADS];
        final List<Exception> threadFailed = new ArrayList<>();

        AtomicInteger count = new AtomicInteger(0);

        for (int i = 0; i < NUM_THREADS; i++) {
            final int ii = i;
            thread[i] = new Thread(new Runnable() {
                    public void run() {
                        try {
                            for (;;)  {
                                Matcher.MergeData md = m.take();
                                if (md == null) {
                                    break;
                                }
                                count.incrementAndGet();
                                w.merge(md.from, md.to);
                                md.onDone.run();
                            }
                        } catch (Exception e) {
                            synchronized(threadFailed) {
                                threadFailed.add(e);
                            }
                        }
                    }
                });
        }
        for (int i = 0; i < NUM_THREADS; i++) {
            thread[i].start();
        }
        int last = m.waitForEnd();
        w.verify(last);

        for (int i = 0; i < NUM_THREADS; i++) {
            thread[i].join();
        }
        Assert.assertEquals(String.format("Failures: %s", threadFailed.toString()),
                            threadFailed.size(), 0);
    }
}
