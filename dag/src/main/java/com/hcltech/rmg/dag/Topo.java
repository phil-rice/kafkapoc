package com.hcltech.rmg.dag;

import java.util.*;

public final class Topo {
    private Topo() {}

    /** Core Kahn’s algorithm over a RequirementGraph. */
    public static <N> List<Set<N>> topoSort(RequirementGraph<N> graph) {
        return topoSort(graph.nodes(), graph.edges());
    }

    /** Convenience: nodes → build graph (validates) → topo sort. */
    public static <N, P> List<Set<N>> topoSortFromNodes(
            Set<N> nodes, PathTC<P> ptc, NodeTC<N, P> ntc) {
        RequirementGraph<N> g = RequirementGraphBuilder.build(nodes, ptc, ntc);
        return topoSort(g);
    }

    // --- internal raw-set variant used by topoSort(graph) ---
    static <N> List<Set<N>> topoSort(Set<N> nodes, Set<Edge<N>> edges) {
        Map<N, Set<N>> adj = new LinkedHashMap<>();
        Map<N, Integer> indeg = new LinkedHashMap<>();
        for (N n : nodes) { adj.put(n, new LinkedHashSet<>()); indeg.put(n, 0); }

        // collapse duplicate (from,to)
        Set<Map.Entry<N,N>> uniq = new LinkedHashSet<>();
        for (Edge<N> e : edges) uniq.add(Map.entry(e.from(), e.to()));
        for (var e : uniq) {
            adj.get(e.getKey()).add(e.getValue());
            indeg.put(e.getValue(), indeg.get(e.getValue()) + 1);
        }

        Deque<N> q = new ArrayDeque<>();
        for (var en : indeg.entrySet()) if (en.getValue() == 0) q.add(en.getKey());

        List<Set<N>> gens = new ArrayList<>();
        int placed = 0;

        while (!q.isEmpty()) {
            Set<N> gen = new LinkedHashSet<>(q); gens.add(gen); q.clear();
            placed += gen.size();
            for (N n : gen) for (N m : adj.getOrDefault(n, Set.of())) indeg.put(m, indeg.get(m) - 1);
            for (var en : indeg.entrySet()) {
                if (en.getValue() == 0 && gens.stream().noneMatch(g -> g.contains(en.getKey()))) q.add(en.getKey());
            }
        }

        if (placed != nodes.size()) {
            Set<N> stuck = new LinkedHashSet<>();
            for (var en : indeg.entrySet()) if (en.getValue() > 0) stuck.add(en.getKey());
            throw new IllegalStateException("Cycle detected among nodes: " + stuck);
        }
        return gens;
    }
}
