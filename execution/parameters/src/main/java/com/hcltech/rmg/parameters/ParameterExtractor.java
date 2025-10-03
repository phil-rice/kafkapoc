package com.hcltech.rmg.parameters;

import com.hcltech.rmg.common.errorsor.ErrorsOr;

import java.util.List;
import java.util.Map;

public interface ParameterExtractor {
    ErrorsOr<Parameters> parameters(Map<String, Object> input, String eventType, String domainType, String domainId);

    static ParameterExtractor defaultParameterExtractor(List<String> parameterNames, Map<String, Map<String, List<String>>> eventToParameterNameToPath, Map<String, List<String>> defaultParameterNameToPath) {
        return new DefaultParameterExtractor(parameterNames, eventToParameterNameToPath, defaultParameterNameToPath);
    }

    static String findValueOrNull(Map<String, Object> message, List<String> path) {
        Object current = message;
        for (String p : path) {
            if (!(current instanceof Map)) return null;
            Map<String, Object> currentMap = (Map<String, Object>) current;
            if (!currentMap.containsKey(p)) return null;
            current = currentMap.get(p);
        }
        if (!(current instanceof String)) return null;
        return (String) current;
    }

}

