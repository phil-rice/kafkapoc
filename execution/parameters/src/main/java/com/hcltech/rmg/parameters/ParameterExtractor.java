package com.hcltech.rmg.parameters;

import com.hcltech.rmg.common.errorsor.ErrorsOr;

import java.util.List;
import java.util.Map;

public interface ParameterExtractor<Msg> {
    ErrorsOr<Parameters> parameters(Msg input, String eventType, String domainType, String domainId);

    static ParameterExtractor<Map<String, Object>> defaultParameterExtractor(List<String> parameterNames, Map<String, Map<String, List<String>>> eventToParameterNameToPath, Map<String, List<String>> defaultParameterNameToPath) {
        return new MapStringObjectParameterExtractor(parameterNames, eventToParameterNameToPath, defaultParameterNameToPath);
    }


}

