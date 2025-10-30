package com.hcltech.rmg.common.function;

@FunctionalInterface
public interface ThrowingFunction<A, T> {
    T apply(A a) throws Exception;
}
