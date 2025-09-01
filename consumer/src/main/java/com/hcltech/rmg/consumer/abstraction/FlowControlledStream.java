package com.hcltech.rmg.consumer.abstraction;

import java.time.Duration;
import java.util.Map;

/**
 * Broker-agnostic source that *already* applies flow control.
 * Messages are native broker type M; shards (S) and positions (P) are native too.
 */
public interface FlowControlledStream<M, S, P> extends AutoCloseable {

    /** Subscribe and receive assignment changes as native shard IDs. */
    void subscribe(String topic, AssignmentListener<S> assignmentListener);

    /**
     * Return messages honoring the supplied Demand:
     * - At most allowance(shard) messages per shard.
     * - At most globalMax messages overall (if > 0).
     * Adapters may buffer internally to satisfy demand precisy.
     */
    Iterable<M> poll(Duration timeout, Demand<S> demand);

    /** Commit/ack to positions per shard (native type P). */
    void commit(Map<S, P> positions);

    /** Whether adapter can truly throttle per-shard at the broker (Kafka=true; EH/Pulsar usually false). */
    boolean supportsPreciseShardFlowControl();

    @Override void close();
}
