package com.hcltech.rmg.config.enrich;

import java.util.List;

public record CompositeExecutor(
        List<EnrichmentAspect> enrichers
) implements EnrichmentAspect {

    @Override
    public List<EnrichmentWithDependencies> asDependencies() {
        return enrichers.stream()
                .flatMap(e ->
                        e.asDependencies().stream())
                .toList();
    }
}
