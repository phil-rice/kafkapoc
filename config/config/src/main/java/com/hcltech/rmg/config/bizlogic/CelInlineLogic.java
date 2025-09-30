package com.hcltech.rmg.config.bizlogic;


import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public record CelInlineLogic(String cel) implements BizLogicAspect {
    @JsonCreator
    public CelInlineLogic(@JsonProperty(value = "cel", required = true) String cel) {
        this.cel = java.util.Objects.requireNonNull(cel, "cel is required");
    }
}
