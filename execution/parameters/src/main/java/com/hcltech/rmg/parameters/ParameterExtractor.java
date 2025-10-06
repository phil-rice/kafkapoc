package com.hcltech.rmg.parameters;

import com.hcltech.rmg.common.errorsor.ErrorsOr;

import java.util.List;
import java.util.Map;

public interface ParameterExtractor {
    ErrorsOr<Parameters> parameters(Map<String, Object> input, String eventType, String domainType, String domainId);

    static ParameterExtractor defaultParameterExtractor(List<String> parameterNames, Map<String, Map<String, List<String>>> eventToParameterNameToPath, Map<String, List<String>> defaultParameterNameToPath) {
        return new DefaultParameterExtractor(parameterNames, eventToParameterNameToPath, defaultParameterNameToPath);
    }



}

