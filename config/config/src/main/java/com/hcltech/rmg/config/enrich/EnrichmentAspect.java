package com.hcltech.rmg.config.enrich;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import java.util.List;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
@JsonSubTypes({
        @JsonSubTypes.Type(value = MapLookupEnrichment.class, name = "lookup"),
        @JsonSubTypes.Type(value = CompositeExecutor.class, name = "composite"),
})
public sealed interface EnrichmentAspect permits MapLookupEnrichment, CompositeExecutor {
    List<EnrichmentWithDependencies> asDependencies();
}
