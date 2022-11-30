package io.lakefs.committer;

import org.apache.hadoop.metrics2.MetricsCollector;
import org.apache.hadoop.metrics2.MetricsInfo;
import org.apache.hadoop.metrics2.MetricsRecordBuilder;
import org.apache.hadoop.metrics2.MetricsSource;
import org.apache.hadoop.metrics2.MetricsSystem;

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.BiConsumer;

public class Metrics implements MetricsSource {
    public static final String NAME = "lakefs.Metrics";
    public static final String DESC = "LakeFSFS metrics";

    public final String context;
    
    private ConcurrentHashMap<String, AtomicLong> metrics = new ConcurrentHashMap<>();

    Metrics(MetricsSystem ms, String context) {
        ms.register(NAME, DESC, this);
        this.context = context;
    }

    long inc(String key) {
        return inc(key, 1);
    }

    long inc(String key, int value) {
        AtomicLong ref = metrics.get(key);
        if (ref == null) {      // Slow path, racing loses an object but not correctness.
            AtomicLong newRef = new AtomicLong();
            ref = metrics.putIfAbsent(key, newRef);
            if (ref == null) {
                // No previous mapping for key, so the new one was added.
                ref = newRef;
            }
        }
        return ref.getAndAdd(value);
    }

    public void getMetrics(MetricsCollector collector, boolean all) {
        MetricsRecordBuilder rb = collector.addRecord("TODO").setContext(context);
        metrics.forEach(new BiConsumer<String, AtomicLong>() {
                @Override
                public void accept(String key, AtomicLong value) {
                    rb.addCounter(new MetricsInfo() {
                            public String name() {
                                return key;
                            }
                            public String description() {
                                return key;
                            }
                        }, value.get());
                    
                }
            });
        rb.endRecord();
    }
}

