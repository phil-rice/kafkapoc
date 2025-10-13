package com.hcltech.rmg.dag;

import org.junit.jupiter.api.Test;

import java.util.Set;

import static com.hcltech.rmg.dag.TestRequirementFixture.*;
import static org.junit.jupiter.api.Assertions.*;

/**
 * RequirementGraphBuilder tests (builder internally validates producers):
 * - single owner builds expected edges
 * - duplicate exact root -> error
 * - prefix-overlap roots -> error
 * - join edges from two distinct producers
 * - external requirement (no owner) -> no edge
 * - self-dependency produces no edge
 * - disconnected nodes don’t affect derived edges
 */
public class RequirementGraphBuilderTest {

    @Test
    void singleOwner_AB_feeds_ABC() {
        // Only AB owns "a.b"; ABC requires "a.b.c" → AB → ABC
        var graph = build(set(AB, ABC)).valueOrThrow();

        assertTrue(graph.edges().contains(new Edge<>(AB, ABC)));
        assertEquals(1, graph.edges().size());
    }

    @Test
    void duplicateExactRoot_isError() {
        // AB and AB_DUP both produce "a.b" → error
        var errs = build(set(AB, AB_DUP, ABC)).errorsOrThrow();
        var msg = String.join("\n", errs);
        assertTrue(msg.contains("Duplicate"), msg);
        assertTrue(msg.contains("a.b"), msg);
    }

    @Test
    void prefixOverlapOwners_isError_a_vs_ab() {
        // A produces "a" and AB produces "a.b" → overlap → error
        var errs = build(set(A, AB, ABC)).errorsOrThrow();
        var msg = String.join("\n", errs);
        assertTrue(msg.contains("Overlapping"), msg);
        assertTrue(msg.contains("a"), msg);
        assertTrue(msg.contains("a.b"), msg);
    }

    @Test
    void prefixOverlapOwners_isError_ab_vs_abc() {
        // "a.b" vs "a.b.c" → overlap → error
        var errs = build(set(AB, ABC_OWNER)).errorsOrThrow();
        var msg = String.join("\n", errs);
        assertTrue(msg.contains("Overlapping"), msg);
        assertTrue(msg.contains("a.b"), msg);
        assertTrue(msg.contains("a.b.c"), msg);
    }

    @Test
    void join_from_B_and_C_into_D() {
        // D requires under b and c → edges B_OWNER→D, C_OWNER→D
        var graph = build(set(B_OWNER, C_OWNER, D_JOIN)).valueOrThrow();

        assertTrue(graph.edges().contains(new Edge<>(B_OWNER, D_JOIN)));
        assertTrue(graph.edges().contains(new Edge<>(C_OWNER, D_JOIN)));
        assertEquals(2, graph.edges().size());
    }

    @Test
    void externalRequirement_createsNoEdge() {
        // EXT requires "z.y", with no producer present
        var graph = build(set(EXT)).valueOrThrow();

        assertTrue(graph.edges().isEmpty());
    }

    @Test
    void selfDependency_isIgnored() {
        // Node owns "a.b" and requires "a.b.c" → no self-edge
        var SELF = node("SELF", new String[]{"a.b"}, new String[]{"a.b.c"});
        var graph = build(set(SELF)).valueOrThrow();

        assertTrue(graph.edges().isEmpty());
    }

    @Test
    void fork_from_A_to_ABX_and_ACY() {
        // A owns "a"; ABX requires "a.b", ACY requires "a.c" → A→ABX and A→ACY
        var graph = build(set(A, ABX, ACY)).valueOrThrow();

        assertEquals(Set.of(new Edge<>(A, ABX), new Edge<>(A, ACY)), graph.edges());
    }

    @Test
    void disconnected_subgraphs_doNotAffectDerivedEdges() {
        // A feeds ABX & ACY; X1 and Y1 are independent producers with no consumers
        var graph = build(set(A, ABX, ACY, X1, Y1)).valueOrThrow();

        assertTrue(graph.edges().contains(new Edge<>(A, ABX)));
        assertTrue(graph.edges().contains(new Edge<>(A, ACY)));
        // No edges related to X1/Y1 (no consumers under x/y)
        assertEquals(Set.of(new Edge<>(A, ABX), new Edge<>(A, ACY)), graph.edges());
    }
}
