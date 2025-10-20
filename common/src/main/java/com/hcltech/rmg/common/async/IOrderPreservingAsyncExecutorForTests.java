package com.hcltech.rmg.common.async;


import java.util.concurrent.Executor;

public interface IOrderPreservingAsyncExecutorForTests<In, Out, FR> extends IOrderPreservingAsyncExecutor<In,Out,FR>{

    /**
     * Drain completions and perform ordered commits using the provided future-record typeclass.
     *
     */
    void drain();

    ILanes<FR,In>  lanes();

    Executor executor();

    CircularBufferWithCallback<Out> outBuffer();

}
