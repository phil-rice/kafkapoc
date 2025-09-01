package com.hcltech.rmg.consumer.abstraction;

import java.util.Map;

/** Per-shard allowances + optional global cap for a poll cycle. */
public final class Demand<S> {
    private final Map<S, Integer> perShard;  // shards not present => allowance 0
    private final int globalMax;             // <= 0 means "no global cap"

    private Demand(Map<S, Integer> perShard, int globalMax) {
        this.perShard = perShard == null ? Map.of() : Map.copyOf(perShard);
        this.globalMax = globalMax;
    }

    public static <S> Demand<S> of(Map<S, Integer> perShard) {
        return new Demand<>(perShard, 0);
    }

    public static <S> Demand<S> of(Map<S, Integer> perShard, int globalMax) {
        return new Demand<>(perShard, globalMax);
    }

    public Map<S, Integer> perShard() { return perShard; }
    public int allowance(S shard) { return perShard.getOrDefault(shard, 0); }
    public int globalMax() { return globalMax; }

    @Override public String toString() {
        return "Demand{perShard=" + perShard + ", globalMax=" + globalMax + "}";
    }
}
