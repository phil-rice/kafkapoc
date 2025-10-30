package com.hcltech.rmg.common.async;

import java.util.function.BiConsumer;
import java.util.function.Consumer;

/**
 * Typeclass/port that knows how to complete a framework "future record".
 * Implementations must be single-use per record (complete only once).
 *
 * Because we are targetting 100K/s throughput and low latency, we need a little complexity here
 * We need to set the key in flink. That is done with something different to the FR. To avoid having
 * to make an object each time, we have the Consumer. This allows us to change the key in flink and update the state
 */
public interface FutureRecordTypeClass<FR, In, Out> {

    /** Complete successfully with a domain value. */
    void completed(FR fr, BiConsumer<In,Out> onComplete, In in,Out out);

    /** Complete with a timeout mapped to a domain value. */
    void timedOut(FR fr,BiConsumer<In,Out> onTimedOut, In in, long elapsedNanos);

    /** Complete with an error mapped to a domain value. */
    void failed(FR fr,BiConsumer<In,Out> onFailed, In in, Throwable error);
}
