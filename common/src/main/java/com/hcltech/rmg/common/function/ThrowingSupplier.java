package com.hcltech.rmg.common.function;

@FunctionalInterface
public interface ThrowingSupplier<T> {
    T get() throws Exception;
}

