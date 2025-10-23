package com.hcltech.rmg.common.copy;

@FunctionalInterface
public interface DeepCopy<T> {
    T copy(T value);
}
