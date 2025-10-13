package com.hcltech.rmg.enrichment;

import com.hcltech.rmg.dag.NodeTC;

import java.util.List;
import java.util.Set;

public final class FixedEnrichmentNodeTC implements NodeTC<FixedEnrichment, List<String>> {

  @Override
  public Set<List<String>> owns(FixedEnrichment n) {
    // This enricher owns exactly its output leaf
    return Set.of(n.output());
  }

  @Override
  public Set<List<String>> requires(FixedEnrichment n) {
    // Inputs are the required leaf paths (order preserved by List)
    return Set.copyOf(n.inputs());
  }

  @Override
  public String label(FixedEnrichment n) {
    // Helpful for diagnostics in planner/topo tests
    return "Fixed(" + String.join(".", n.output()) + ")";
  }
}
