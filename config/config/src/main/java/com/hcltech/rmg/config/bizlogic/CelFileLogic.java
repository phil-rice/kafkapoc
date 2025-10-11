package com.hcltech.rmg.config.bizlogic;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;

public record CelFileLogic(String type, String file) implements BizLogicAspect {
    @JsonCreator
    public CelFileLogic(@JsonProperty(value = "file", required = true) String file) {
        this(BizLogicAspect.celFileType, Objects.requireNonNull(file, BizLogicAspect.celFileType + " is required"));
    }
}