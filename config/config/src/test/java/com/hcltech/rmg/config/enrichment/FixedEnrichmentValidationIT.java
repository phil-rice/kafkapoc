package com.hcltech.rmg.config.enrichment;

import org.junit.jupiter.api.Test;

import java.util.List;

import static com.hcltech.rmg.config.enrichment.dag.FixedEnrichmentFixture.*;
import static com.hcltech.rmg.dag.ProducerValidation.validate;
import static org.junit.jupiter.api.Assertions.*;

public class FixedEnrichmentValidationIT {

    @Test
    void distinctOutputs_areValid() {
        assertEquals(Boolean.TRUE, validate(set(A, B, C, D, E, X, Y), ptc(), ntc()).valueOrThrow());
    }

    @Test
    void duplicateOutput_isError() {
        // B and B_DUP both produce ["b"]
        List<String> errs = validate(set(B, B_DUP), ptc(), ntc()).errorsOrThrow();
        assertFalse(errs.isEmpty(), "Expected duplicate output validation errors");
        // Optional: message shape
        // assertTrue(String.join("\n", errs).contains("Duplicate"));
    }

    @Test
    void prefixOverlapOutputs_isError() {
        // ["b"] vs ["b","child"]
        List<String> errs = validate(set(B, B_CHILD), ptc(), ntc()).errorsOrThrow();
        assertFalse(errs.isEmpty(), "Expected prefix-overlap validation errors");
        // Optional:
        // assertTrue(String.join("\n", errs).contains("Overlapping"));
    }

    @Test
    void noProducers_isValid() {
        // Only a consumer; validator checks producers only
        assertEquals(Boolean.TRUE, validate(set(Z_CONSUMER), ptc(), ntc()).valueOrThrow());
    }
}
