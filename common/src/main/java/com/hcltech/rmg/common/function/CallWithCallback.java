package com.hcltech.rmg.common.function;

@FunctionalInterface
public interface CallWithCallback<In, Out> {
    /**
     * Non-blocking contract. Sync impls may invoke the callback immediately.
     */
    void call(In in, Callback<? super Out> cb);
}
