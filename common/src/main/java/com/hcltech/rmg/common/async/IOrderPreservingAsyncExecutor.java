package com.hcltech.rmg.common.async;

/**
 * Minimal, framework-agnostic async executor interface.
 *
 * <p>Single-threaded contract: all calls are made from the operator thread.</p>
 *
 * @param <In>  type of input items
 * @param <Out> type of output items
 * @param <FR>  type of framework "future record" that tracks async completions. In practice probably a Flink FutureRecord<Out>
 */
public interface IOrderPreservingAsyncExecutor<In, Out, FR> {

   void add(In input, FR futureRecord);

    /**
     * Finish/close the executor. After calling this, new adds are ignored and late completions are dropped safely.
     */
    void close();
}
