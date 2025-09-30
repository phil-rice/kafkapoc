package com.hcltech.rmg.config.enrich;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Map;

public record ApiEnrichment(String api, Map<String,String> params) implements EnrichmentAspect {
    @JsonCreator
    public  ApiEnrichment(@JsonProperty(value = "api", required = true) String api, @JsonProperty(value = "params", required = true) Map<String,String> params) {
        this.api = java.util.Objects.requireNonNull(api, "api is required");
        this.params = params == null ? Map.of() : params;
    }
}
