package com.hcltech.rmg.shared_worker;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

public final class FirstHitFlatMap<T> implements FlatMapFunction<T, T> {

    private final AtomicBoolean firstSeen;

    public static AtomicBoolean defaultFirstSeen = new AtomicBoolean(false);

    public FirstHitFlatMap(AtomicBoolean firstSeen) {
        this.firstSeen = firstSeen;
    }

    @Override
    public void flatMap(T value, Collector<T> out) {
        if (firstSeen.compareAndSet(false, true)) {
            out.collect(value); // emit exactly the first element globally (single JVM)
        }
    }
}
