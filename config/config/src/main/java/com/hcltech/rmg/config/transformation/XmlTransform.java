package com.hcltech.rmg.config.transformation;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public record XmlTransform(String schema) implements TransformationAspect {
    @JsonCreator
    public XmlTransform(@JsonProperty(value = "schema", required = true) String schema) {
        this.schema = java.util.Objects.requireNonNull(schema, "schema is required");
    }
}
