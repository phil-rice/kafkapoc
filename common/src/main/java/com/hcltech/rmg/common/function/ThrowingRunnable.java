package com.hcltech.rmg.common.function;

@FunctionalInterface
public interface ThrowingRunnable {
    void run() throws Exception;
}
