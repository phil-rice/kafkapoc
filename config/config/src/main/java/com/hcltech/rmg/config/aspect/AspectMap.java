package com.hcltech.rmg.config.aspect;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.hcltech.rmg.config.bizlogic.BizLogicAspect;
import com.hcltech.rmg.config.enrich.EnrichmentAspect;
import com.hcltech.rmg.config.transformation.TransformationAspect;
import com.hcltech.rmg.config.validation.ValidationAspect;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

@JsonIgnoreProperties(ignoreUnknown = true)
public record AspectMap(
        Map<String, ValidationAspect> validation,
        Map<String, TransformationAspect> transformation,
        Map<String, EnrichmentAspect> enrichment,
        Map<String, BizLogicAspect> bizlogic
) implements Serializable {
    @JsonCreator
    public AspectMap(
            @JsonProperty("validation") Map<String, ValidationAspect> validation,
            @JsonProperty("transformation") Map<String, TransformationAspect> transformation,
            @JsonProperty("enrichment") Map<String, EnrichmentAspect> enrichment,
            @JsonProperty("bizlogic") Map<String, BizLogicAspect> bizlogic
    ) {
        this.validation = validation == null ? Map.of() : validation;
        this.transformation = transformation == null ? Map.of() : transformation;
        this.enrichment = enrichment == null ? Map.of() : enrichment;
        this.bizlogic = bizlogic == null ? Map.of() : bizlogic;
    }

    public static AspectMap empty() {
        return new AspectMap(Map.of(), Map.of(), Map.of(), Map.of());
    }
}
