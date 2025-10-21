package com.hcltech.rmg.flinkadapters;

import com.hcltech.rmg.common.async.FailureAdapter;
import com.hcltech.rmg.common.async.FutureRecordTypeClass;
import com.hcltech.rmg.messages.RetryEnvelope;
import org.apache.flink.util.Collector;

import java.util.Objects;
import java.util.function.Supplier;

/**
 * FutureRecordTypeClass that emits to the CURRENT Flink Collector
 * via a thread-local set by the keyed operator around exec.drain().
 *
 * <FR> is ignored (use a trivial marker when enqueuing).
 * <p>
 * Usage from your KeyedProcessFunction:
 * CollectorFutureRecordAdapter.use(outCollector, () -> {
 * exec.drain();  // all emits inside drain go to 'outCollector'
 * });
 */
public final class FlinkCollectorFutureRecordAdapter<In, Out>
        implements FutureRecordTypeClass<Collector<Out>, In, Out> {

    private final FailureAdapter<In, Out> failureAdapter;

    public FlinkCollectorFutureRecordAdapter(FailureAdapter<In, Out> failureAdapter) {
        this.failureAdapter = failureAdapter;
    }

    @Override
    public void completed(Collector<Out> outCollector, Out out) {
        outCollector.collect(out);
    }

    @Override
    public void timedOut(Collector<Out> outCollector, In in, long elapsedNanos) {
        outCollector.collect(failureAdapter.onTimeout(in, elapsedNanos));

    }

    @Override
    public void failed(Collector<Out> outCollector, In in, Throwable error) {
        outCollector.collect(failureAdapter.onFailure(in, error));
    }
}
