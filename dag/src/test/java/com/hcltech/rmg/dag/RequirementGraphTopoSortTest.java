package com.hcltech.rmg.dag;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Set;

import static com.hcltech.rmg.dag.TestRequirementFixture.*;
import static org.junit.jupiter.api.Assertions.*;

public class RequirementGraphTopoSortTest {

  @Test
  void singleOwner_AB_then_ABC() {
    // AB owns "a.b"; ABC requires "a.b.c"
    var gens = Topo.topoSortFromNodes(set(AB, ABC), ptc(), ntc);
    assertEquals(List.of(Set.of(AB), Set.of(ABC)), gens);
  }

  @Test
  void fork_A_to_ABX_and_ACY() {
    // A owns "a"; ABX requires "a.b"; ACY requires "a.c"
    var gens = Topo.topoSortFromNodes(set(A, ABX, ACY), ptc(), ntc);
    assertEquals(2, gens.size());
    assertEquals(Set.of(A), gens.get(0));
    assertEquals(Set.of(ABX, ACY), gens.get(1));
  }

  @Test
  void join_B_and_C_into_D() {
    // B→D and C→D
    var gens = Topo.topoSortFromNodes(set(B_OWNER, C_OWNER, D_JOIN), ptc(), ntc);
    assertEquals(List.of(Set.of(B_OWNER, C_OWNER), Set.of(D_JOIN)), gens);
  }

  @Test
  void disconnected_subgraphs_sortedIndependently() {
    // A→ABX, plus independent producers X1 and Y1
    var gens = Topo.topoSortFromNodes(set(A, ABX, X1, Y1), ptc(), ntc);
    assertEquals(2, gens.size());
    assertTrue(gens.get(0).containsAll(Set.of(A, X1, Y1))); // all sources
    assertEquals(Set.of(ABX), gens.get(1));
  }

  @Test
  void noEdges_allInFirstGen() {
    var gens = Topo.topoSortFromNodes(set(X1, Y1), ptc(), ntc);
    assertEquals(List.of(Set.of(X1, Y1)), gens);
  }

  @Test
  void duplicateProducer_isError() {
    // AB & AB_DUP both produce "a.b" → builder (via validator) throws
    assertThrows(IllegalArgumentException.class,
        () -> Topo.topoSortFromNodes(set(AB, AB_DUP, ABC), ptc(), ntc));
  }

  @Test
  void prefixOverlapProducers_isError() {
    // A produces "a" and AB produces "a.b" → overlap → error
    assertThrows(IllegalArgumentException.class,
        () -> Topo.topoSortFromNodes(set(A, AB, ABC), ptc(), ntc));
  }

  @Test
  void cycle_isDetected_afterEdgesDerived() {
    // A needs under C; B under A; C under B
    assertThrows(IllegalStateException.class,
        () -> Topo.topoSortFromNodes(set(A_NEEDS_C, B_NEEDS_A, C_NEEDS_B), ptc(), ntc));
  }
}
