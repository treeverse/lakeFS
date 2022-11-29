package io.lakefs.utils;

import net.bytebuddy.matcher.ElementMatchers;

import static org.junit.Assert.*;
import org.junit.Before;
import org.junit.Test;

public class MetricsInterceptorTest {
    protected static class TestException extends RuntimeException {
        TestException() {
            super("You know, for tests!");
        }
    }

    protected static class Bar {
        public long wait1(long msecs) throws InterruptedException {
            Thread.sleep(msecs);
            return msecs;
        }

        public long wait2(long halfMsecs) throws InterruptedException {
            long msecs = 2 * halfMsecs;
            Thread.sleep(msecs);
            return msecs;
        }

        public long waitThrows(long msecs) throws InterruptedException {
            Thread.sleep(msecs);
            throw new TestException();
        }
    }

    protected MetricsInterceptor barInterceptor;
    protected Class<Bar> wrappedBar;

    @Before
    public void wrapClasses() {
        makeWrapped(Bar.class);
    }

    private void makeWrapped(Class clazz) {
        barInterceptor = new MetricsInterceptor();
        wrappedBar = barInterceptor.wrap(clazz, ElementMatchers.nameStartsWith("wait"));
    }

    @Test
    public void testWrapper() throws InterruptedException, InstantiationException, IllegalAccessException {
        // Note "note" on Class.newInstance: doing this bypasses
        // compile-time checking on exceptions declared thrown by the
        // constructor!
        Bar bar = wrappedBar.newInstance();
        long msecs = 0;
        msecs += bar.wait1(500);
        msecs += bar.wait2(250);
        msecs += bar.wait1(400);

        MetricsInterceptor.Metrics metrics = barInterceptor.getMetrics();

        assertEquals(3, metrics.numCalls);
        assertTrue("at least total sleep duration", msecs <= metrics.time);
        assertTrue("not more than 2* sleep duration",  metrics.time <= 2*msecs);
    }

    public void testWrapperThrows() throws Exception {
        Bar bar = wrappedBar.newInstance();
        try {
            bar.waitThrows(150);
        } catch (TestException unused) {
        }

        MetricsInterceptor.Metrics metrics = barInterceptor.getMetrics();

        assertEquals(1, metrics.numCalls);
        assertTrue("at least total sleep duration", 150 <= metrics.time);
        assertTrue("not more than 2* sleep duration",  metrics.time <= 2*150);
    }
}
