package com.hcltech.rmg.flinkadapters;

import com.hcltech.rmg.common.async.*;

import java.io.Serializable;
import java.util.Objects;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;

/**
 * Immutable configuration for {@link AsyncCepProcessFunction}.
 *
 * @param perKeyCapK     Per-key concurrency cap (and ring capacity baseline)
 * @param subtaskCapM    Subtask-wide concurrency cap (SemaphorePermitManager)
 * @param asyncFn        Core async function producing CompletionStage<Out>
 * @param failureAdapter Adapter to map (Throwable, In) → Out on failure
 * @param inSeq          HasSeq typeclass for input type
 * @param outSeq         HasSeq typeclass for output type
 */
public record AsyncCepConfig<In, Out>(
        int perKeyCapK,
        int subtaskCapM,
        Function<In, CompletionStage<Out>> asyncFn,
        FailureAdapter<In, Out> failureAdapter,
        HasSeq<In> inSeq,
        HasSeq<Out> outSeq
) implements Serializable {

    public AsyncCepConfig {
        if (perKeyCapK <= 0)
            throw new IllegalArgumentException("perKeyCapK must be > 0");
        if (subtaskCapM <= 0)
            throw new IllegalArgumentException("subtaskCapM must be > 0");
        Objects.requireNonNull(asyncFn, "asyncFn");
        Objects.requireNonNull(failureAdapter, "failureAdapter");
        Objects.requireNonNull(inSeq, "inSeq");
        Objects.requireNonNull(outSeq, "outSeq");
    }

    @Override
    public String toString() {
        return "AsyncCepConfig[" +
                "K=" + perKeyCapK +
                ", M=" + subtaskCapM +
                ", asyncFn=" + asyncFn.getClass().getSimpleName() +
                ", failureAdapter=" + failureAdapter.getClass().getSimpleName() +
                ']';
    }
}
