package com.hcltech.rmg.flinkadapters;

import com.hcltech.rmg.common.async.FailureAdapter;
import com.hcltech.rmg.common.async.FutureRecordTypeClass;
import org.apache.flink.streaming.api.functions.async.ResultFuture;

import java.io.Serializable;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeoutException;

/** Completes Flink ResultFuture with domain results or exceptions. */
public final class FlinkResultFutureAdapter<In, Out>
        implements FutureRecordTypeClass<ResultFuture<Out>, In, Out>, Serializable {

    private final FailureAdapter<In, Out> failureAdapter;
    private final boolean completeExceptionallyOnFailure;
    private final boolean completeExceptionallyOnTimeout;

    public FlinkResultFutureAdapter(FailureAdapter<In, Out> failureAdapter) {
        this(failureAdapter, false, false);
    }

    public FlinkResultFutureAdapter(FailureAdapter<In, Out> failureAdapter,
                                    boolean completeExceptionallyOnFailure,
                                    boolean completeExceptionallyOnTimeout) {
        this.failureAdapter = Objects.requireNonNull(failureAdapter, "failureAdapter");
        this.completeExceptionallyOnFailure = completeExceptionallyOnFailure;
        this.completeExceptionallyOnTimeout = completeExceptionallyOnTimeout;
    }

    @Override
    public void completed(ResultFuture<Out> fr, Out out) {
        Objects.requireNonNull(fr, "fr");
        Objects.requireNonNull(out, "out");
        fr.complete(List.of(out));
    }

    @Override
    public void timedOut(ResultFuture<Out> fr, In in, long elapsedNanos) {
        Objects.requireNonNull(fr, "fr");
        if (completeExceptionallyOnTimeout) {
            fr.completeExceptionally(new TimeoutException("elapsedNanos=" + elapsedNanos));
            return;
        }
        Out out = Objects.requireNonNull(failureAdapter.onTimeout(in, elapsedNanos),
                                         "FailureAdapter.onTimeout returned null");
        fr.complete(List.of(out));
    }

    @Override
    public void failed(ResultFuture<Out> fr, In in, Throwable error) {
        Objects.requireNonNull(fr, "fr");
        Objects.requireNonNull(error, "error");
        if (completeExceptionallyOnFailure) {
            fr.completeExceptionally(error);
            return;
        }
        Out out = Objects.requireNonNull(failureAdapter.onFailure(in, error),
                                         "FailureAdapter.onFailure returned null");
        fr.complete(List.of(out));
    }
}
