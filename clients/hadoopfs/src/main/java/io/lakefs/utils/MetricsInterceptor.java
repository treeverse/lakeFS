package io.lakefs.utils;

import net.bytebuddy.*;
import net.bytebuddy.implementation.MethodDelegation;
import net.bytebuddy.implementation.bind.annotation.Origin;
import net.bytebuddy.implementation.bind.annotation.RuntimeType;
import net.bytebuddy.implementation.bind.annotation.SuperCall;
import net.bytebuddy.matcher.ElementMatcher;

import java.lang.reflect.Method;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.Callable;

// (Based on ByteBuddy TimingInterceptor example.)
public class MetricsInterceptor {
    private AtomicLong totalNumCalls = new AtomicLong();
    private AtomicLong totalTime = new AtomicLong();

    public static class Metrics {
        long numCalls;
        long time;
    }

    public Class wrap(Class clazz, ElementMatcher matcher) {
        return new ByteBuddy()
            .subclass(clazz)
            .name(String.format("%s$wrapped", clazz.getName()))
            .method(matcher)
            .intercept(MethodDelegation.to(this))
            .make()
            .load(getClass().getClassLoader())
            .getLoaded();
    }

    @RuntimeType
    public Object intercept(@Origin Method method,
                            @SuperCall Callable<?> callable) throws Exception {
        long start = System.currentTimeMillis();
        try {
            return callable.call();
        } catch (Exception e) {
            throw(e);
        } finally {
            long durationMsecs = System.currentTimeMillis() - start;
            System.out.println(method + " took " + durationMsecs);
            totalNumCalls.addAndGet(1);
            totalTime.addAndGet(durationMsecs);
        }
    }

    // Does *not* fetch atomically, values may be mismatched!
    public Metrics getMetrics() {
        Metrics ret = new Metrics();
        ret.numCalls = totalNumCalls.get();
        ret.time = totalTime.get();
        return ret;
    }
}
