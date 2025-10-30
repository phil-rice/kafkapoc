package com.hcltech.rmg.dag;

import java.util.Collection;
import java.util.Optional;

public interface PathTC<P> {
    /** Segment-aware prefix: e.g. "a.b" ⊑ "a.b.c", but not ⊑ "a.bc". */
    boolean isPrefix(P prefix, P full);

    /** Total order; make deeper paths compare greater (primary by depth, then lexicographic). */
    int compare(P a, P b);

    /** Helper: pick the deepest owner root that prefixes the required path. */
    default Optional<P> nearestOwner(P required, Collection<P> allRoots) {
        P best = null;
        for (P root : allRoots) {
            if (isPrefix(root, required)) {
                if (best == null || compare(root, best) > 0) best = root;
            }
        }
        return Optional.ofNullable(best);
    }
}
