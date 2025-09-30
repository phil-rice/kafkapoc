package com.hcltech.rmg.config.validation;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public record CelValidation(String cel) implements ValidationAspect {
    @JsonCreator
    public CelValidation(@JsonProperty(value = "cel", required = true) String cel) {
        this.cel = java.util.Objects.requireNonNull(cel, "schema is required");
    }
}
