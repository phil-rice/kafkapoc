package com.hcltech.rmg.config.transformation;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public record XsltTransform(String file, String schema) implements TransformationAspect {
    @JsonCreator
    public XsltTransform(@JsonProperty(value = "file", required = true) String file,@JsonProperty(value = "schema", required = true) String schema) {
        this.file = java.util.Objects.requireNonNull(file, "file is required");
        this.schema = java.util.Objects.requireNonNull(schema, "schema is required");
    }
}
