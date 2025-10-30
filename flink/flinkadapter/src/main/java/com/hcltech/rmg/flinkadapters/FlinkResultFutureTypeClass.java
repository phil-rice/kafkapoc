package com.hcltech.rmg.flinkadapters;

import com.hcltech.rmg.common.async.FailureAdapter;
import com.hcltech.rmg.common.async.FutureRecordTypeClass;
import org.apache.flink.streaming.api.functions.async.ResultFuture;

import java.io.Serializable;
import java.util.Collections;
import java.util.Objects;
import java.util.concurrent.TimeoutException;
import java.util.function.BiConsumer;

/**
 * FutureRecordTypeClass that completes a Flink ResultFuture with a single result.
 * Hooks (onComplete/onFailed/onTimedOut) run on the operator thread before completing.
 */
public final class FlinkResultFutureTypeClass<In, Out>
        implements FutureRecordTypeClass<ResultFuture<Out>, In, Out>, Serializable {

    private static final long serialVersionUID = 1L;

    private final FailureAdapter<In, Out> failureAdapter;

    public FlinkResultFutureTypeClass(FailureAdapter<In, Out> failureAdapter) {
        this.failureAdapter = Objects.requireNonNull(failureAdapter, "failureAdapter");
    }

    @Override
    public void completed(ResultFuture<Out> fr,
                          BiConsumer<In, Out> onComplete,
                          In in,
                          Out out) {
        Objects.requireNonNull(fr, "fr");
        Objects.requireNonNull(out, "out");
        if (onComplete != null) onComplete.accept(in, out);
        fr.complete(Collections.singletonList(out));
    }

    @Override
    public void timedOut(ResultFuture<Out> fr,
                         BiConsumer<In, Out> onTimedOut,
                         In in,
                         long elapsedNanos) {
        Objects.requireNonNull(fr, "fr");
        Out mapped = failureAdapter.onTimeout(in, elapsedNanos);
        if (mapped != null) {
            if (onTimedOut != null) onTimedOut.accept(in, mapped);
            fr.complete(Collections.singletonList(mapped));
        } else {
            // Fallback: signal an actual timeout if no mapped value
            fr.completeExceptionally(new TimeoutException("Async timed out after " + elapsedNanos + " ns"));
        }
    }

    @Override
    public void failed(ResultFuture<Out> fr,
                       BiConsumer<In, Out> onFailed,
                       In in,
                       Throwable error) {
        Objects.requireNonNull(fr, "fr");
        Objects.requireNonNull(error, "error");
        Out mapped = failureAdapter.onFailure(in, error);
        if (mapped != null) {
            if (onFailed != null) onFailed.accept(in, mapped);
            fr.complete(Collections.singletonList(mapped));
        } else {
            // Fallback: propagate the failure if there is no mapping
            fr.completeExceptionally(error);
        }
    }
}
