package com.hcltech.rmg.consumer.abstraction;

import java.util.concurrent.ThreadFactory;

/**
 * Broker-agnostic worker configuration.
 */
public interface WorkerConfig<S,P> {

    /** Topic / stream name (single topic per worker). */
    String topic();

    /** Poll timeout in ms (how long we wait in poll loop). */
    int pollMs();

    /** Commit tick cadence in ms. */
    long commitTickMs();

    /** Runner buffer capacity (batch size per shard). */
    int runnerBufferCapacity();

    /** Thread factory for per-shard "lane" threads. */
    ThreadFactory laneFactory();

    /** Client id base (for metrics/logging). */
    String clientId();

    SeekStrategy<S,P> seekStrategy();

    // --- broker-specific delegation ---
    // E.g. Kafka would expose group.id, bootstrap.servers etc.
    // For hexagonal, keep them in the adapter's config, not here.
}
