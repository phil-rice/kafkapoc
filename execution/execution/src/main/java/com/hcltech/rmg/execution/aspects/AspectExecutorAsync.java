package com.hcltech.rmg.execution.aspects;

import com.hcltech.rmg.common.function.Callback;

/**
 * Unified async-shaped port for all aspects.
 * <p>
 * Re-entrancy notice: Implementations are allowed to invoke the callback
 * inline (synchronously) before returning from {@code call}. Callers must be
 * prepared for re-entrant callback execution and should avoid assumptions
 * about threading or asynchrony.
 */
@FunctionalInterface
public interface AspectExecutorAsync<Component, Inp, Out> {
    void call(String key, Component component, Inp input, Callback<? super Out> cb);
}
