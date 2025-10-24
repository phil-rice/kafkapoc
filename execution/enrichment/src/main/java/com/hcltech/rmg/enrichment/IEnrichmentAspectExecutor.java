package com.hcltech.rmg.enrichment;

import com.hcltech.rmg.cepstate.CepEvent;
import com.hcltech.rmg.cepstate.CepStateTypeClass;
import com.hcltech.rmg.common.errorsor.ErrorsOr;
import com.hcltech.rmg.common.function.CallWithCallback;
import com.hcltech.rmg.config.configs.Configs;
import com.hcltech.rmg.config.configs.ConfigsVisitor;
import com.hcltech.rmg.config.configs.ConfigsWalker;
import com.hcltech.rmg.config.enrich.EnrichmentAspect;
import com.hcltech.rmg.config.enrich.EnrichmentWithDependencies;
import com.hcltech.rmg.config.enrich.EnrichmentWithDependenciesNodeTc;
import com.hcltech.rmg.dag.ListPathTC;
import com.hcltech.rmg.dag.Topo;
import com.hcltech.rmg.execution.aspects.AspectExecutor;
import com.hcltech.rmg.execution.aspects.AspectExecutorAsync;
import com.hcltech.rmg.messages.ValueEnvelope;

import java.util.HashSet;
import java.util.Set;

/**
 * Executes all configured enrichments (generation-ordered) for a given envelope.
 * Implementations MUST be non-blocking: invoke the provided callback exactly once,
 * either synchronously (inline) or asynchronously when work completes.
 */
public interface IEnrichmentAspectExecutor<CepState, Msg>
        extends CallWithCallback<ValueEnvelope<CepState, Msg>, ValueEnvelope<CepState, Msg>> {

    static <CepState, Msg> ErrorsOr<IEnrichmentAspectExecutor<CepState, Msg>> create(CepStateTypeClass<CepState> cepStateTypeClass, Configs configs, AspectExecutorAsync<EnrichmentWithDependencies, ValueEnvelope<CepState, Msg>, CepEvent> evaluator) {
        Set<EnrichmentWithDependencies> dependencies = new HashSet<>();
        ConfigsWalker.walk(configs, new ConfigsVisitor() {
            @Override
            public void onEnrichment(String paramKey, String eventName, String moduleName, EnrichmentAspect e) {
                dependencies.addAll(e.asDependencies());
            }
        });
        return Topo.topoSortFromNodes(dependencies, ListPathTC.INSTANCE, EnrichmentWithDependenciesNodeTc.INSTANCE).map(generations ->
                new EnrichmentAspectExecutor<CepState, Msg>(cepStateTypeClass, generations, evaluator));
    }
}
