package com.hcltech.rmg.common.async;


import java.util.concurrent.Executor;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

public interface IOrderPreservingAsyncExecutorForTests<In, Out, FR> extends IOrderPreservingAsyncExecutor<In,Out,FR>{

    /**
     * Drain completions and perform ordered commits using the provided future-record typeclass.
     *
     */
    public void drain(final FR fr, final BiConsumer<In, Out> hook);

    ILanes<In>  lanes();

    Executor executor();

}
