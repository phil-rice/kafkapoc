package com.hcltech.rmg.config.bizlogic;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
@JsonSubTypes({
        @JsonSubTypes.Type(value = CelFileLogic.class, name = "celFile"),
        @JsonSubTypes.Type(value = CelInlineLogic.class, name = "cel")
})
public sealed interface BizLogicAspect permits CelFileLogic, CelInlineLogic {
}
