package com.hcltech.rmg.common;

public interface ITimeService {
    long currentTimeNanos();

    ITimeService real = System::nanoTime;

    static ITimeService fixed(long fixedTimeMillis) {
        return () -> fixedTimeMillis;
    }
}
