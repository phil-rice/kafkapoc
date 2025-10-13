package com.hcltech.rmg.dag;

import java.util.*;

/** Immutable dependency graph: nodes + (from→to) edges. */
public record RequirementGraph<N>(Set<N> nodes, Set<Edge<N>> edges) {
  public RequirementGraph {
    nodes = Collections.unmodifiableSet(new LinkedHashSet<>(nodes));
    edges = Collections.unmodifiableSet(new LinkedHashSet<>(edges));
  }
}
