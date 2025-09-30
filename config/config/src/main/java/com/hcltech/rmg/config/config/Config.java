package com.hcltech.rmg.config.config;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.hcltech.rmg.config.aspect.AspectMap;

import java.util.Map;

@JsonIgnoreProperties(ignoreUnknown = true)
public record Config(Map<String, AspectMap> events) {
    public final static String validationAspectName="validation";
    public final static String transformationAspectName="transformation";
    public final static String bizlogicAspectName="bizlogic";

    public static final String configKey(String moduleName,String aspectName, String eventName) {
        return moduleName+":"+aspectName+":"+eventName;
    }

    @JsonCreator
    public Config(@JsonProperty(value = "events", required = true) Map<String, AspectMap> events) {
        this.events = events == null ? Map.of() : events;
    }

    public static Config empty() {
        return new Config(Map.of());
    }
}
