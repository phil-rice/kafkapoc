package com.hcltech.rmg.config.enrich;

import java.util.List;
import java.util.Map;

/**
 * Support inputs was List.of(List.of("a"), List.of("b")) and lookup was Map.of("a1.b1", 1, "a1.b2", 2, "a2.b1", 3, "a2.b2", 4) and output is X
 * Then when the msg comes in we find a  and b in the input, join with a dot and look it up
 * <p>
 * For example {"a": "a1", "b": "b2"} will enrich with 2 giving {"a": "a1", "b": "b2", "X": 2}
 */
public record MapLookupEnrichment(
        List<List<String>> inputs,
        List<String> output,
        Map<String, Object> lookup
) implements EnrichmentAspect, EnrichmentWithDependencies {

    @Override
    public List<EnrichmentWithDependencies> asDependencies() {
        return List.of(this);
    }
}
