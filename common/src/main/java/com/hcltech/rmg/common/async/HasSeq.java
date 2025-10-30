package com.hcltech.rmg.common.async;

/**
 * Type class for sequence tagging. Implementations may mutate and return the same instance,
 * or return a NEW instance carrying the given sequence.
 */
public interface HasSeq<T> {
    long get(T value);
    T set(T value, long seq);
}
