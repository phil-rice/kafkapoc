package com.hcltech.rmg.config.bizlogic;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public record CelFileLogic(String file) implements BizLogicAspect {
    @JsonCreator
    public CelFileLogic(@JsonProperty(value = "file", required = true) String file) {
        this.file = java.util.Objects.requireNonNull(file, "file is required");
    }
}