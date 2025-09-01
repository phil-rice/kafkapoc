package com.hcltech.rmg.consumer.abstraction;

import java.util.Map;
import java.util.Set;

/**
 * Pure policy: from per-shard RunnerState (capacity/outstanding) and immutable
 * configuration, decide per-shard credits for the next poll.
 */
public interface DemandCalculator<S, R extends RunnerState> {
    Demand<S> compute(Set<S> assigned, Map<S, R> state);
}
