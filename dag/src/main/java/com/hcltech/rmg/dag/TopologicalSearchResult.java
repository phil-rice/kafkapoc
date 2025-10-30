package com.hcltech.rmg.dag;

import java.util.*;

/** Immutable result you can display or inspect. */
public record TopologicalSearchResult<N>(
    Set<N> nodes,
    Set<Edge<N>> edges,
    List<Set<N>> generations
) {}
