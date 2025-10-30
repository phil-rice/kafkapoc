package com.hcltech.rmg.config.bizlogic;


import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;

public record CelInlineLogic(String type, String cel) implements BizLogicAspect {
    @JsonCreator
    public CelInlineLogic(@JsonProperty(value = "cel", required = true) String cel) {
        this(BizLogicAspect.celInlineType, Objects.requireNonNull(cel, BizLogicAspect.celInlineType + " is required"));
    }
}
