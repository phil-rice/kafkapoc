package com.hcltech.rmg.dag;

import java.util.Set;

public interface NodeTC<N, P> {
    /** Ownership roots (each root claims its entire subtree). */
    Set<P> owns(N node);

    /** Required leaf paths. */
    Set<P> requires(N node);

    /** Label for logs/tests (defaults to toString). */
    default String label(N node) { return String.valueOf(node); }
}
