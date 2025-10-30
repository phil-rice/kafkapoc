package com.hcltech.rmg.dag;

import com.hcltech.rmg.common.errorsor.ErrorsOr;

import java.util.*;

public final class RequirementGraphBuilder {
    private RequirementGraphBuilder() {}

    public static <N, P> ErrorsOr<RequirementGraph<N>> build(Set<N> nodes, PathTC<P> ptc, NodeTC<N, P> ntc) {
        Objects.requireNonNull(nodes);
        Objects.requireNonNull(ptc);
        Objects.requireNonNull(ntc);

        // Validate producer policy (exact dup + prefix overlap -> error)
        return ProducerValidation.validate(nodes, ptc, ntc).map(v -> {
            // Deterministic owner choice (still useful if policy is later relaxed)
            List<N> ordered = new ArrayList<>(nodes);
            ordered.sort(Comparator.comparing(ntc::label));

            Map<P, N> ownerOf = new LinkedHashMap<>();
            for (N n : ordered) {
                for (P root : ntc.owns(n)) {
                    ownerOf.putIfAbsent(root, n);
                }
            }

            // Build edges via nearest (deepest) owner
            Set<Edge<N>> edges = new LinkedHashSet<>();
            Set<P> allRoots = ownerOf.keySet();
            for (N consumer : nodes) {
                for (P req : ntc.requires(consumer)) {
                    ptc.nearestOwner(req, allRoots).ifPresent(best -> {
                        N producer = ownerOf.get(best);
                        if (!producer.equals(consumer)) {
                            edges.add(new Edge<>(producer, consumer));
                        }
                    });
                }
            }

            return new RequirementGraph<>(nodes, edges);
        });
    }
}
