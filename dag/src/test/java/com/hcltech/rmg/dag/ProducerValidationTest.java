package com.hcltech.rmg.dag;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Set;

import static com.hcltech.rmg.dag.TestRequirementFixture.*;
import static org.junit.jupiter.api.Assertions.*;

/**
 * ProducerValidation tests:
 * - exact duplicate produced root -> error
 * - prefix-overlap produced roots (a.b vs a.b.c) -> error
 * - distinct produced roots -> OK
 * - empty / no producers -> OK
 */
public class ProducerValidationTest {

    @Test
    void duplicateExactProducer_isError() {
        // Both produce "a.b"
        Set<TestRequirementNode> nodes = set(AB, AB_DUP);

        List<String> errs = ProducerValidation.validate(nodes, ptc(), ntc).errorsOrThrow();
        String msg = String.join("\n", errs);
        assertTrue(msg.contains("Duplicate"), msg);
        assertTrue(msg.contains("a.b"), msg);
    }

    @Test
    void prefixOverlapProducer_isError() {
        // "a.b" vs "a.b.c"
        Set<TestRequirementNode> nodes = set(AB, ABC_OWNER);

        List<String> errs = ProducerValidation.validate(nodes, ptc(), ntc).errorsOrThrow();
        String msg = String.join("\n", errs);
        assertTrue(msg.contains("Overlapping"), msg);
        assertTrue(msg.contains("a.b"), msg);
        assertTrue(msg.contains("a.b.c"), msg);
    }

    @Test
    void distinctProducers_areValid() {
        // a, b, c are disjoint
        Set<TestRequirementNode> nodes = set(A, B_OWNER, C_OWNER);

        // Success path yields Boolean.TRUE (non-null)
        assertEquals(Boolean.TRUE, ProducerValidation.validate(nodes, ptc(), ntc).valueOrThrow());
    }

    @Test
    void noProducers_isValid() {
        // Only consumers; no produced roots to validate
        Set<TestRequirementNode> nodes = set(ABC, ABX, ACY, EXT);

        assertEquals(Boolean.TRUE, ProducerValidation.validate(nodes, ptc(), ntc).valueOrThrow());
    }

    @Test
    void manyDistinctDeepBranches_areValid() {
        var P1 = node("P1", new String[]{"m.n"},     new String[]{});
        var P2 = node("P2", new String[]{"m.x"},     new String[]{});
        var P3 = node("P3", new String[]{"q.r.s"},   new String[]{});
        var P4 = node("P4", new String[]{"u"},       new String[]{});
        Set<TestRequirementNode> nodes = set(P1, P2, P3, P4);

        assertEquals(Boolean.TRUE, ProducerValidation.validate(nodes, ptc(), ntc).valueOrThrow());
    }

    @Test
    void errorListsBothConflictingProducers() {
        // Verify error message references both producers by label
        var P1 = node("LEFT",  new String[]{"k.l"},   new String[]{});
        var P2 = node("RIGHT", new String[]{"k.l.m"}, new String[]{});

        List<String> errs = ProducerValidation.validate(set(P1, P2), ptc(), ntc).errorsOrThrow();
        String msg = String.join("\n", errs);
        assertTrue(msg.contains("LEFT"), msg);
        assertTrue(msg.contains("RIGHT"), msg);
        assertTrue(msg.contains("Overlapping"), msg);
    }
}
