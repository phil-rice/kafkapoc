package com.hcltech.rmg.config.enrichment.dag;

import com.hcltech.rmg.config.enrich.FixedEnrichment;
import com.hcltech.rmg.config.enrich.dag.FixedEnrichmentNodeTC;
import com.hcltech.rmg.dag.Edge;
import com.hcltech.rmg.dag.ListPathTC;
import com.hcltech.rmg.dag.RequirementGraphBuilder;
import com.hcltech.rmg.dag.Topo;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Set;

import static java.util.List.of;
import static org.junit.jupiter.api.Assertions.*;

public class AddressEnrichmentPlanIT {

    private static List<String> p(String... segs) {
        return List.of(segs);
    }

    @Test
    void postcode_then_suffix_then_instructions_generations_and_edges() {
        // 1) Postcode from address lines (all 5 present even if empty)
        var POSTCODE_FROM_ADDR = new FixedEnrichment(
                of(p("addr", "line1"), p("addr", "line2"), p("addr", "line3"), p("addr", "city"), p("addr", "country")),
                p("addr", "postcode"),
                java.util.Map.of()
        );
        // 2) Postcode suffix from (line1, line2, postcode)
        var SUFFIX_FROM_L12_PC = new FixedEnrichment(
                of(p("addr", "line1"), p("addr", "line2"), p("addr", "postcode")),
                p("addr", "postcodeSuffix"),
                java.util.Map.of()
        );
        // 3) Delivery instructions from (postcode, postcodeSuffix)
        var INSTR_FROM_PC_SUFF = new FixedEnrichment(
                of(p("addr", "postcode"), p("addr", "postcodeSuffix")),
                p("delivery", "instructions"),
                java.util.Map.of()
        );

        // Build graph and sort (builder validates producers internally)
        var graph = RequirementGraphBuilder.build(
                Set.of(POSTCODE_FROM_ADDR, SUFFIX_FROM_L12_PC, INSTR_FROM_PC_SUFF),
                new ListPathTC(),
                new FixedEnrichmentNodeTC()
        ).valueOrThrow();

        // Assert the whole generations list in one go
        assertEquals(
                List.of(
                        Set.of(POSTCODE_FROM_ADDR), // generation 1
                        Set.of(SUFFIX_FROM_L12_PC), // generation 2
                        Set.of(INSTR_FROM_PC_SUFF)  // generation 3
                ),
                Topo.topoSort(graph).valueOrThrow()
        );

        // Assert edges for clarity (optional, but nice)
        assertEquals(
                Set.of(
                        new Edge<>(POSTCODE_FROM_ADDR, SUFFIX_FROM_L12_PC),
                        new Edge<>(POSTCODE_FROM_ADDR, INSTR_FROM_PC_SUFF),
                        new Edge<>(SUFFIX_FROM_L12_PC, INSTR_FROM_PC_SUFF)
                ),
                graph.edges()
        );
    }

    @Test
    void ridiculous_bad_config_cycle_between_postcode_and_suffix_shows_nice_error() {
        // Intentionally wrong:
        // - Postcode depends on postcodeSuffix
        // - PostcodeSuffix depends on postcode
        // Unique outputs, so validation passes; topo should detect a cycle.

        var POSTCODE_FROM_SUFFIX = new FixedEnrichment(
                of(p("addr", "postcodeSuffix")),            // requires suffix
                p("addr", "postcode"),                      // produces postcode
                java.util.Map.of()
        );
        var SUFFIX_FROM_POSTCODE = new FixedEnrichment(
                of(p("addr", "postcode")),                  // requires postcode
                p("addr", "postcodeSuffix"),                // produces suffix
                java.util.Map.of()
        );

        // Build graph (no producer overlap, so it builds), then expect topo to fail with a cycle
        var graph = RequirementGraphBuilder.build(
                Set.of(POSTCODE_FROM_SUFFIX, SUFFIX_FROM_POSTCODE),
                new ListPathTC(),
                new FixedEnrichmentNodeTC()
        ).valueOrThrow();

        var errs = Topo.topoSort(graph).errorsOrThrow();
        var msg = String.join("\n", errs);
        assertTrue(msg.toLowerCase().contains("cycle"), "Expected a cycle error, got: " + msg);
    }
}
