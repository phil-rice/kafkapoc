package com.hcltech.rmg.config.enrichment.dag;

import com.hcltech.rmg.dag.Topo;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Set;

import static com.hcltech.rmg.config.enrichment.dag.FixedEnrichmentFixture.*;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration tests: nodes -> RequirementGraph (builder & validator) -> Kahn generations.
 */
public class FixedEnrichmentTopoSortIT {

  @Test
  void chain_A_to_B_to_C() {
    // A → B → C
    var gens = Topo.topoSortFromNodes(set(A, B, C), ptc(), ntc());
    assertEquals(List.of(Set.of(A), Set.of(B), Set.of(C)), gens);
  }

  @Test
  void fork_A_to_B_and_D() {
    // A → (B, D)
    var gens = Topo.topoSortFromNodes(set(A, B, D), ptc(), ntc());
    assertEquals(2, gens.size());
    assertEquals(Set.of(A), gens.get(0));
    assertEquals(Set.of(B, D), gens.get(1));
  }

  @Test
  void join_B_and_D_into_E() {
    // (B, D) → E
    var gens = Topo.topoSortFromNodes(set(B, D, E), ptc(), ntc());
    assertEquals(List.of(Set.of(B, D), Set.of(E)), gens);
  }

  @Test
  void fork_then_join_full_path() {
    // A → (B, D) → E
    var gens = Topo.topoSortFromNodes(set(A, B, D, E), ptc(), ntc());
    assertEquals(3, gens.size());
    assertEquals(Set.of(A), gens.get(0));
    assertEquals(Set.of(B, D), gens.get(1));
    assertEquals(Set.of(E), gens.get(2));
  }

  @Test
  void disconnected_subgraphs_sources_first() {
    // A → ABX, plus independent producers X and Y
    var gens = Topo.topoSortFromNodes(set(A, ABX, X, Y), ptc(), ntc());
    assertEquals(2, gens.size());
    assertTrue(gens.get(0).containsAll(Set.of(A, X, Y))); // all zero-indegree sources
    assertEquals(Set.of(ABX), gens.get(1));               // A's dependent
  }

  @Test
  void external_requirement_stays_in_first_generation() {
    // No producer for "z" → Z_CONSUMER has no incoming edges
    var gens = Topo.topoSortFromNodes(set(Z_CONSUMER), ptc(), ntc());
    assertEquals(List.of(Set.of(Z_CONSUMER)), gens);
  }

  @Test
  void cycle_detected_after_projection() {
    // Make an explicit 3-node cycle: A' requires c → produces a; B' requires a → produces b; C' requires b → produces c
    var Aprime = fe(List.of(path("c")), path("a"));
    var Bprime = fe(List.of(path("a")), path("b"));
    var Cprime = fe(List.of(path("b")), path("c"));

    // Builder derives edges A'←C', B'←A', C'←B' which forms a cycle
    assertThrows(IllegalStateException.class, () -> Topo.topoSortFromNodes(set(Aprime, Bprime, Cprime), ptc(), ntc()));
  }
}
