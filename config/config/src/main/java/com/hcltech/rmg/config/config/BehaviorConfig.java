package com.hcltech.rmg.config.config;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.hcltech.rmg.config.aspect.AspectMap;

import java.io.Serializable;
import java.util.*;

@JsonIgnoreProperties(ignoreUnknown = true)
/** The events is a map from the event name to the AspectMap for that event. */
public record BehaviorConfig(Map<String, AspectMap> events) implements Serializable {
    public final static String validationAspectName = "validation";
    public final static String transformationAspectName = "transformation";
    public final static String bizlogicAspectName = "bizlogic";

    public static final String configKey(String moduleName, String aspectName, String eventName) {
        return moduleName + ":" + aspectName + ":" + eventName;
    }

    @JsonCreator
    public BehaviorConfig(@JsonProperty(value = "events", required = true) Map<String, AspectMap> events) {
        this.events = events == null ? Map.of() : events;
    }

    public List<String> allModuleNames() {
        Set<String> set = new HashSet<>();
        for (AspectMap as : events.values()) {
            set.addAll(as.bizlogic().keySet());
            set.addAll(as.validation().keySet());
            set.addAll(as.transformation().keySet());
        }
        List<String> result = new ArrayList<>(set);
        result.sort(String::compareTo);
        return result;
    }

    public static BehaviorConfig empty() {
        return new BehaviorConfig(Map.of());
    }
}
