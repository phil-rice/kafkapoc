package com.hcltech.rmg.config.bizlogic;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

import java.io.Serializable;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
@JsonSubTypes({
        @JsonSubTypes.Type(value = CelFileLogic.class, name = BizLogicAspect.celFileType),
        @JsonSubTypes.Type(value = CelInlineLogic.class, name = BizLogicAspect.celInlineType)
})
public sealed interface BizLogicAspect  extends Serializable permits CelFileLogic, CelInlineLogic {
    public static final String celInlineType = "cel";
    public static final String celFileType = "celFile";
}
