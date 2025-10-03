package com.hcltech.rmg.config.config;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.hcltech.rmg.config.aspect.AspectMap;

import java.util.Map;

@JsonIgnoreProperties(ignoreUnknown = true)
public record BehaviorConfig(Map<String, AspectMap> events) {
    public final static String validationAspectName="validation";
    public final static String transformationAspectName="transformation";
    public final static String bizlogicAspectName="bizlogic";

    public static final String configKey(String moduleName,String aspectName, String eventName) {
        return moduleName+":"+aspectName+":"+eventName;
    }

    @JsonCreator
    public BehaviorConfig(@JsonProperty(value = "events", required = true) Map<String, AspectMap> events) {
        this.events = events == null ? Map.of() : events;
    }

    public static BehaviorConfig empty() {
        return new BehaviorConfig(Map.of());
    }
}
