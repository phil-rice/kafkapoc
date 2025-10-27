package com.hcltech.rmg.config.enrich;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

import java.util.List;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
@JsonSubTypes({
        @JsonSubTypes.Type(value = MapLookupEnrichment.class, name = "lookup"),
        @JsonSubTypes.Type(value = CompositeExecutor.class, name = "composite"),
        @JsonSubTypes.Type(value = FixedEnrichment.class, name = "fixed"),
        @JsonSubTypes.Type(value = CsvEnrichment.class, name = "csv"),
})
public sealed interface EnrichmentAspect permits MapLookupEnrichment, CompositeExecutor, FixedEnrichment,CsvEnrichment,ApiEnrichment {
    List<EnrichmentWithDependencies> asDependencies();
}
