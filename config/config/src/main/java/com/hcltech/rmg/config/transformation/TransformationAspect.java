package com.hcltech.rmg.config.transformation;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
@JsonSubTypes({
        @JsonSubTypes.Type(value = XsltTransform.class, name = "xml")
})
public sealed interface TransformationAspect permits XmlTransform, XsltTransform {
}
