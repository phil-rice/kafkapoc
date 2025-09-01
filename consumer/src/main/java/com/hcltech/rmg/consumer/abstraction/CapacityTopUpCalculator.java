package com.hcltech.rmg.consumer.abstraction;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Top-up per shard up to remainingCapacity, limited by perPoll cap
 * and a soft outstanding ceiling. behavior suitable for most cases
 */
public final class CapacityTopUpCalculator<S, R extends RunnerState>
        implements DemandCalculator<S, R> {

    private final DemandCalculatorConfig cfg;
    private final ProcessorProfile<S> profileOrNull;


    public CapacityTopUpCalculator(DemandCalculatorConfig cfg, ProcessorProfile<S> profileOrNull) {
        this.cfg = cfg;
        this.profileOrNull = profileOrNull;
    }

    @Override
    public Demand<S> compute(Set<S> assigned, Map<S, R> state) {
        Map<S, Integer> perShard = new HashMap<>();
        int total = 0;

        for (S s : assigned) {
            R rs = state.get(s);
            if (rs == null) continue;

            int cap = Math.max(0, rs.remainingCapacity());
            if (cap == 0) continue;

            int configuredMaxOut = cfg.maxOutstandingPerPartition();
            int hintedMaxOut = (profileOrNull != null)
                    ? Math.max(1, Math.min(configuredMaxOut, profileOrNull.maxOutstandingHint(s, configuredMaxOut)))
                    : configuredMaxOut;

            int credit = Math.max(0, hintedMaxOut - rs.outstanding());
            if (credit == 0) continue;

            int need = Math.min(cap, Math.min(cfg.perPollPerPartition(), credit));
            if (need > 0) {
                perShard.put(s, need);
                total += need;
            }
        }

        int globalMax = cfg.globalMax() > 0 ? Math.min(total, cfg.globalMax()) : total;
        return perShard.isEmpty() ? Demand.of(Map.of(), 0) : Demand.of(perShard, globalMax);
    }
}
