package com.hcltech.rmg.common;

public interface ITimeService {
    long currentTimeMillis();

    ITimeService real = System::currentTimeMillis;

    static ITimeService fixed(long fixedTimeMillis) {
        return () -> fixedTimeMillis;
    }
}
