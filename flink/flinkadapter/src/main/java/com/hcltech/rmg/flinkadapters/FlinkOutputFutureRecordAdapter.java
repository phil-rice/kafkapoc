package com.hcltech.rmg.flinkadapters;

import com.hcltech.rmg.common.async.FailureAdapter;
import com.hcltech.rmg.common.async.FutureRecordTypeClass;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import java.util.Objects;
import java.util.function.BiConsumer;

/**
 * FutureRecordTypeClass that emits to a Flink {@code Output<StreamRecord<Out>>}.
 *
 * <p>Contract:
 * <ul>
 *   <li>Called on the operator thread (during executor drain).</li>
 *   <li>Runs the provided BiConsumer hook (e.g., set key + update CEP state) <b>before</b> emitting.</li>
 *   <li>Maps failures/timeouts to {@code Out} via {@link FailureAdapter} and emits those too.</li>
 * </ul>
 *
 * <p>Note: This variant does <b>not</b> preserve event-time timestamps; it emits
 * {@code new StreamRecord<>(out)}. If you need to keep upstream timestamps,
 * carry the ts in your {@code Out} (or a side channel) and replace the
 * {@code collect(...)} with {@code new StreamRecord<>(out, ts)}.</p>
 */
public final class FlinkOutputFutureRecordAdapter<In, Out>
        implements FutureRecordTypeClass<Output<StreamRecord<Out>>, In, Out> {

    private final FailureAdapter<In, Out> failureAdapter;

    public FlinkOutputFutureRecordAdapter(FailureAdapter<In, Out> failureAdapter) {
        this.failureAdapter = Objects.requireNonNull(failureAdapter, "failureAdapter");
    }

    @Override
    public void completed(Output<StreamRecord<Out>> output,
                          BiConsumer<In, Out> onComplete,
                          In in,
                          Out out) {
        onComplete.accept(in, out);
        output.collect(new StreamRecord<>(out));
    }

    @Override
    public void timedOut(Output<StreamRecord<Out>> output,
                         BiConsumer<In, Out> onTimedOut,
                         In in,
                         long elapsedNanos) {
        Out mapped = failureAdapter.onTimeout(in, elapsedNanos);
        onTimedOut.accept(in, mapped);
        output.collect(new StreamRecord<>(mapped));
    }

    @Override
    public void failed(Output<StreamRecord<Out>> output,
                       BiConsumer<In, Out> onFailed,
                       In in,
                       Throwable error) {
        Out mapped = failureAdapter.onFailure(in, error);
        onFailed.accept(in, mapped);
        output.collect(new StreamRecord<>(mapped));
    }
}
