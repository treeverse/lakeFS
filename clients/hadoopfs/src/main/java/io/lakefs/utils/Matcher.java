package io.lakefs.utils;

import java.util.Collection;
import java.util.Queue;
import java.util.Set;
import java.util.function.Consumer;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Implementation of a multithreaded Matcher.  It holds the logic underlying
 * a concurrent merge operation.  Initialized with the range of an index to
 * objects, it offers pairs of indices <from, to> and expects from to be
 * merged into to then returned.  The take operation is structured to allow
 * safe multithreaded merging to work.
 *
 * Matcher itself is thread-safe by being a point of contention.  So it is
 * suited when the merge operation itself takes a long time, for instance
 * when it requires network access.
 */
public class Matcher {
    private Lock lock = new ReentrantLock();
    private Condition changed = lock.newCondition();
    private volatile boolean done = false;
    private int last = -1;
    private Exception exception;
    private Queue<Integer> toMerge;
    private Set<Integer> inProcess;

    public static class FailedException extends Exception {
        FailedException(String message, Throwable cause) {
            super(message, cause);
        }
    }

    /**
     * A pair for merging returned to the application.  It should merge from
     * into to and then call done or fail.  from and to are unavailable for
     * use in other pairs until that happens.  If done is called, from is
     * considered merged into to, to becomes available to other callers, and
     * waiting callers will be released.  If fail is called, all waiting
     * callers will be failed with that exception.
     */
    public static class MergeData {
        public int from;
        public int to;
        public Runnable onDone;
        public Consumer<Exception> onFail;
    }

    public Matcher(int size) {
        toMerge = new java.util.ArrayDeque<>(size);
        for (int i = 0; i < size; i++) {
            toMerge.add(i);
        }
        inProcess = new java.util.HashSet<>();
    }

    private MergeData tryTakeLocked() {
        if (toMerge.size() <= 1 && inProcess.size() == 0) {
            done = true;
            return null;
        }
        if (toMerge.size() <= 1) {
            return null;
        }
        int from = toMerge.remove();
        int to = toMerge.remove();
        inProcess.add(from);
        inProcess.add(to);
        MergeData md = new MergeData();
        md.from = from;
        md.to = to;
        md.onDone = new Runnable() {
                public void run() {
                    processMerged(from, to);
                }
            };
        md.onFail = new Consumer<Exception>() {
                @Override
                public void accept(Exception e) {
                    processFailed(e, from, to);
                }
            };
        return md;
    }

    public MergeData take() throws FailedException {
        lock.lock();
        try {
            while (!done) {
                if (exception != null) {
                    throw new FailedException("Matching failed", exception);
                }
                MergeData ret = tryTakeLocked();
                if (ret != null || done) {
                    changed.signalAll();
                    return ret;
                }
                changed.await();
            }
            return null;
        } catch (InterruptedException e) {
            throw new FailedException("interrupted while take()ing", e);
        } finally {
            lock.unlock();
        }
    }

    private void processMerged(int from, int to) {
        lock.lock();
        try {
            inProcess.remove(from);
            inProcess.remove(to);
            toMerge.offer(to);
            changed.signal();
        } finally {
            lock.unlock();
        }
    }

    private void processFailed(Exception e, int from, int to) {
        lock.lock();
        try {
            done = true;
            exception = e;
            inProcess.remove(from);
            inProcess.remove(to);
            changed.signal();
        } finally {
            lock.unlock();
        }
    }

    public int waitForEnd() throws FailedException, InterruptedException {
        lock.lock();
        try {
            while (!done) {
                changed.await();
            }
            if (exception != null) {
                throw new FailedException("execution failed", exception);
            }
            if (last >= 0) {
                return last;
            }
            if (toMerge.isEmpty()) {
                throw new FailedException("failed to determine last merged", null);
            }
            last = toMerge.remove();
            return last;
        } finally {
            lock.unlock();
        }
    }
}
