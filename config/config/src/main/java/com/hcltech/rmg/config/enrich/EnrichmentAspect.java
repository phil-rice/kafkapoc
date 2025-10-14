package com.hcltech.rmg.config.enrich;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import java.util.List;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
@JsonSubTypes({
        @JsonSubTypes.Type(value = FixedEnrichment.class, name = "fixed"),
        @JsonSubTypes.Type(value = CompositeExecutor.class, name = "composite"),
})
public sealed interface EnrichmentAspect permits FixedEnrichment, CompositeExecutor {
    List<EnrichmentWithDependencies> asDependencies();
}
