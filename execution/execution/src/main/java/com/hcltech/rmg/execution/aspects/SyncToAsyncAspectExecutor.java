package com.hcltech.rmg.execution.aspects;

/**
 * Adapter that wraps a synchronous {@link AspectExecutor} and exposes an async-shaped API.
 * Implementations may invoke the callback inline (synchronously).
 */
public final class SyncToAsyncAspectExecutor<Component, Inp, Out>
        implements AspectExecutorAsync<Component, Inp, Out> {

    private final AspectExecutor<Component, Inp, Out> sync;

    public SyncToAsyncAspectExecutor(AspectExecutor<Component, Inp, Out> sync) {
        this.sync = java.util.Objects.requireNonNull(sync, "executor");
    }

    @Override
    public void call(String key, Component component, Inp input,
                     com.hcltech.rmg.common.function.Callback<? super Out> cb) {
        try {
            cb.success(sync.execute(key, component, input));
        } catch (Throwable t) {
            cb.failure(t);
        }
    }
}
