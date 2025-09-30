package com.hcltech.rmg.config.enrich;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
@JsonSubTypes({
        @JsonSubTypes.Type(value = ApiEnrichment.class, name = "api")
})
public sealed interface EnrichmentAspect permits ApiEnrichment {
}
