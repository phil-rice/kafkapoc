package com.hcltech.rmg.worker.eventhubtest;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicLong;

public class LogWhatWeHaveFunction<T> extends RichMapFunction<T, T> {
    private static final Logger log = LoggerFactory.getLogger(LogWhatWeHaveFunction.class);

    private static AtomicLong counter = new AtomicLong(0);
    private static final AtomicLong start = new AtomicLong(0);

    @Override
    public T map(T o) throws Exception {
        long count = counter.getAndIncrement();
        if (count == 0) {
            start.set(System.currentTimeMillis());
            log.info(count + ": " + o.toString());
        } else {
            if (count % 1000 == 0) {
                long now = System.currentTimeMillis();
                long duration = now - start.get();
                double rate = (count * 1000.0) / duration;
                log.info("Processed " + count + " records in " + duration + " ms (" + String.format("%.2f", rate) + " records/sec)");
            }
        }
        return o;
    }
}
