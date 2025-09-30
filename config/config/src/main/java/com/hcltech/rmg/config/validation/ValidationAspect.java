package com.hcltech.rmg.config.validation;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
@JsonSubTypes({
        @JsonSubTypes.Type(value = CelValidation.class, name = "cel"),
})
public sealed interface ValidationAspect permits  CelValidation {
}
