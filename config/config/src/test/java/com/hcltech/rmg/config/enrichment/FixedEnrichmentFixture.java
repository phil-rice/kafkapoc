package com.hcltech.rmg.config.enrichment;


import com.hcltech.rmg.config.enrich.EnrichmentAspect;
import com.hcltech.rmg.config.enrich.EnrichmentWithDepedencies;
import com.hcltech.rmg.config.enrich.EnrichmentWithDepedenciesNodeTc;
import com.hcltech.rmg.config.enrich.FixedEnrichment;
import com.hcltech.rmg.dag.ListPathTC;
import com.hcltech.rmg.dag.NodeTC;
import com.hcltech.rmg.dag.PathTC;

import java.util.*;

import static java.util.Arrays.asList;

/**
 * Reusable fixture for FixedEnrichment integration tests (validation, build, topo).
 */
public final class FixedEnrichmentFixture {

    // ---------- Typeclasses ----------
    public static PathTC<List<String>> ptc() {
        return new ListPathTC();
    }

    public static NodeTC<EnrichmentWithDepedencies, List<String>> ntc() {
        return EnrichmentWithDepedenciesNodeTc.INSTANCE;
    }

    // ---------- Path / set helpers ----------
    public static List<String> path(String... segs) {
        return List.of(segs);
    }

    public static List<List<String>> paths(String[]... segsArr) {
        List<List<String>> out = new ArrayList<>();
        for (String[] segs : segsArr) out.add(path(segs));
        return out;
    }

    @SafeVarargs
    public static <T> Set<T> set(T... xs) {
        return new LinkedHashSet<>(asList(xs));
    }

    // ---------- Node builders ----------
    public static FixedEnrichment fe(List<List<String>> inputs, List<String> output) {
        return new FixedEnrichment(inputs, output, Map.of());
    }

    public static FixedEnrichment fe(List<List<String>> inputs, List<String> output, Map<String, Object> lookup) {
        return new FixedEnrichment(inputs, output, lookup);
    }

    // ---------- Canonical nodes ----------

    // Chain: A -> B -> C
    // A: produces ["a"]
    public static final FixedEnrichment A = fe(List.of(), path("a"));
    // B: requires ["a"], produces ["b"]
    public static final FixedEnrichment B = fe(List.of(path("a")), path("b"));
    // C: requires ["a","b"], produces ["c"]
    public static final FixedEnrichment C = fe(List.of(path("a"), path("b")), path("c"));

    // Fork from A, then join into E
    // D: requires ["a"], produces ["d"]
    public static final FixedEnrichment D = fe(List.of(path("a")), path("d"));
    // E: requires ["b","d"], produces ["e"]
    public static final FixedEnrichment E = fe(List.of(path("b"), path("d")), path("e"));

    // Disconnected producers
    public static final FixedEnrichment X = fe(List.of(), path("x"));
    public static final FixedEnrichment Y = fe(List.of(), path("y"));

    // External need (no owner for "z")
    public static final FixedEnrichment Z_CONSUMER = fe(List.of(path("z")), path("z_out"));

    // Validation conflict cases
    // Duplicate output "b" — make record structurally different to avoid Set dedup (non-empty lookup)
    public static final FixedEnrichment B_DUP = fe(List.of(path("a")), path("b"), Map.of("dummy", 1));
    // Prefix overlap: "b" vs "b.child"
    public static final FixedEnrichment B_CHILD = fe(List.of(path("a")), path("b", "child"));

    // Extra consumers for fork examples (require under "a", produce independent leaves)
    public static final FixedEnrichment ABX = fe(List.of(path("a")), path("abx"));
    public static final FixedEnrichment ACY = fe(List.of(path("a")), path("acy"));

    private FixedEnrichmentFixture() {
    }
}
