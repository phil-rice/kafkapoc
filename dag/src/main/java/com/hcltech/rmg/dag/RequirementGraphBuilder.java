package com.hcltech.rmg.dag;

import java.util.*;

public final class RequirementGraphBuilder {
  private RequirementGraphBuilder() {}

  public static <N, P> RequirementGraph<N> build(Set<N> nodes, PathTC<P> ptc, NodeTC<N, P> ntc) {
    Objects.requireNonNull(nodes); Objects.requireNonNull(ptc); Objects.requireNonNull(ntc);

    // Enforce producer policy first (exact dup + prefix overlap -> error)
    ProducerValidation.validate(nodes, ptc, ntc);

    // Deterministic owner choice (still needed for duplicate roots if you later relax policy)
    List<N> ordered = new ArrayList<>(nodes);
    ordered.sort(Comparator.comparing(ntc::label));

    Map<P, N> ownerOf = new LinkedHashMap<>();
    for (N n : ordered) for (P root : ntc.owns(n)) ownerOf.putIfAbsent(root, n);

    // Build edges via nearest (deepest) owner
    Set<Edge<N>> edges = new LinkedHashSet<>();
    Set<P> allRoots = ownerOf.keySet();
    for (N consumer : nodes) {
      for (P req : ntc.requires(consumer)) {
        var best = ptc.nearestOwner(req, allRoots);
        if (best.isPresent()) {
          N producer = ownerOf.get(best.get());
          if (!producer.equals(consumer)) edges.add(new Edge<>(producer, consumer));
        }
      }
    }

    return new RequirementGraph<>(nodes, edges);
  }
}
