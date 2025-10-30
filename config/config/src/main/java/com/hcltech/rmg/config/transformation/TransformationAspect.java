package com.hcltech.rmg.config.transformation;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
@JsonSubTypes({
        @JsonSubTypes.Type(value = XmlTransform.class, name = "xml"),
        @JsonSubTypes.Type(value = XsltTransform.class, name = "xslt")
})
public sealed interface TransformationAspect permits XmlTransform, XsltTransform {
}

