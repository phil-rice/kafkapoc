package com.hcltech.rmg.worker.eventhubtest;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicLong;
public class LogWhatWeHaveFunction<T> extends RichMapFunction<T, T> {
    private static final Logger log = LoggerFactory.getLogger(LogWhatWeHaveFunction.class);

    // shared across all parallel subtasks in the same JVM (ok for embedded)
    private static final AtomicLong total = new AtomicLong(0);
    private static final AtomicLong lastCount = new AtomicLong(0);
    private static final AtomicLong lastTimeMs = new AtomicLong(System.currentTimeMillis());

    private static final long LOG_EVERY = 100_000;

    @Override
    public T map(T o) {
        long c = total.incrementAndGet();
        if (c == 1) {
            lastCount.set(0);
            lastTimeMs.set(System.currentTimeMillis());
            log.info("0: {}", String.valueOf(o));
        } else if (c % LOG_EVERY == 0) {
            long now = System.currentTimeMillis();
            long prevC = lastCount.getAndSet(c);
            long prevT = lastTimeMs.getAndSet(now);
            long deltaC = c - prevC;
            long deltaT = Math.max(1, now - prevT);
            double instRate = (deltaC * 1000.0) / deltaT;
            log.info("Processed {} (+{}) in {} ms ({} rec/s inst)",
                    c, deltaC, deltaT, String.format("%.0f", instRate));
        }
        return o;
    }
}
