package com.hcltech.rmg.enrichment;

import com.hcltech.rmg.cepstate.CepEvent;
import com.hcltech.rmg.cepstate.CepStateTypeClass;
import com.hcltech.rmg.common.errorsor.ErrorsOr;
import com.hcltech.rmg.common.function.Callback;
import com.hcltech.rmg.config.configs.Configs;
import com.hcltech.rmg.config.configs.ConfigsVisitor;
import com.hcltech.rmg.config.configs.ConfigsWalker;
import com.hcltech.rmg.config.enrich.EnrichmentAspect;
import com.hcltech.rmg.config.enrich.EnrichmentWithDependencies;
import com.hcltech.rmg.config.enrich.EnrichmentWithDependenciesNodeTc;
import com.hcltech.rmg.dag.ListPathTC;
import com.hcltech.rmg.dag.Topo;
import com.hcltech.rmg.execution.aspects.AspectExecutorAsync;
import com.hcltech.rmg.execution.aspects.AspectExecutorSync;
import com.hcltech.rmg.execution.aspects.SyncToAsyncAspectExecutor;
import com.hcltech.rmg.messages.ValueEnvelope;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Async-shaped enrichment executor. One invocation may be in-flight per lane.
 *
 * @param <CepState> CEP state type
 * @param <Msg>      message type
 */
public interface IEnrichmentAspectExecutor<CepState, Msg> {

    /**
     * Execute the enrichment pipeline for a single input on the given lane.
     * Implementations must invoke exactly one of {@code cb.success} or {@code cb.failure}.
     */
    void call(int laneId, ValueEnvelope<CepState, Msg> input,
              Callback<? super ValueEnvelope<CepState, Msg>> cb);

    /**
     * Build an async, lane-aware enrichment executor.
     *
     * @param laneCount              number of concurrent lanes; executor pre-allocates one lane state each
     * @param expectedEventsPerSlot   capacity hint for per-slot event list (purely a perf hint; use a small constant like 2–4)
     * @param evaluator               either an {@link AspectExecutorAsync} of {@code List<CepEvent>} or an {@link AspectExecutorSync} wrapped via {@link SyncToAsyncAspectExecutor}
     */
    static <CepState, Msg> ErrorsOr<IEnrichmentAspectExecutor<CepState, Msg>> create(
            CepStateTypeClass<CepState> cepStateTypeClass,
            Configs configs,
            Object evaluator,
            int laneCount,
            int expectedEventsPerSlot
    ) {
        // Discover enrichment graph
        Set<EnrichmentWithDependencies> deps = new HashSet<>();
        ConfigsWalker.walk(configs, new ConfigsVisitor() {
            @Override
            public void onEnrichment(String paramKey, String eventName, String moduleName, EnrichmentAspect e) {
                deps.addAll(e.asDependencies());
            }
        });

        // Normalise evaluator to async
        final AspectExecutorAsync<EnrichmentWithDependencies, ValueEnvelope<CepState, Msg>, CepEvent> asyncEval;
        if (evaluator instanceof AspectExecutorAsync<?, ?, ?> aa) {
            @SuppressWarnings("unchecked")
            var cast = (AspectExecutorAsync<EnrichmentWithDependencies, ValueEnvelope<CepState, Msg>, CepEvent>) aa;
            asyncEval = cast;
        } else if (evaluator instanceof AspectExecutorSync<?, ?, ?> ss) {
            @SuppressWarnings("unchecked")
            var sync = (AspectExecutorSync<EnrichmentWithDependencies, ValueEnvelope<CepState, Msg>, CepEvent>) ss;
            asyncEval = new SyncToAsyncAspectExecutor<>(sync);
        } else {
            throw new IllegalArgumentException("Unsupported evaluator type: " + evaluator.getClass());
        }

        // Build lane-aware executor over topo-sorted generations
        return Topo.topoSortFromNodes(deps, ListPathTC.INSTANCE, EnrichmentWithDependenciesNodeTc.INSTANCE)
                .map(gens -> new EnrichmentAspectExecutor<>(
                        cepStateTypeClass,
                        gens,
                        asyncEval,
                        /* laneCount */ Math.max(1, laneCount),
                        /* expectedEventsPerSlot */ Math.max(1, expectedEventsPerSlot)
                ));
    }
}
