package com.hcltech.rmg.config.enrich;


import com.hcltech.rmg.dag.NodeTC;

import java.util.List;
import java.util.Set;

public final class EnrichmentWithDependenciesNodeTc implements NodeTC<EnrichmentWithDependencies, List<String>> {

    public static final EnrichmentWithDependenciesNodeTc INSTANCE = new EnrichmentWithDependenciesNodeTc();

    private EnrichmentWithDependenciesNodeTc() {
        // Singleton
    }

    @Override
    public Set<List<String>> owns(EnrichmentWithDependencies n) {
        // This enricher owns exactly its output leaf
        return Set.of(n.output());
    }

    @Override
    public Set<List<String>> requires(EnrichmentWithDependencies n) {
        // Inputs are the required leaf paths (order preserved by List)
        return Set.copyOf(n.inputs());
    }

    @Override
    public String label(EnrichmentWithDependencies n) {
        // Helpful for diagnostics in planner/topo tests
        return "Fixed(" + String.join(".", n.output()) + ")";
    }
}
