package com.hcltech.rmg.parameters;

import com.hcltech.rmg.common.errorsor.ErrorsOr;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

class DefaultParameterExtractor implements ParameterExtractor {

    private final List<String> parameterNames;
    private final Map<String, Map<String, List<String>>> eventToParameterNameToPath;
    private final Map<String, List<String>> defaultParameterNameToPath;

    public DefaultParameterExtractor(List<String> parameterNames, Map<String, Map<String, List<String>>> eventToParameterNameToPath, Map<String, List<String>> defaultParameterNameToPath) {
        this.parameterNames = parameterNames;
        this.eventToParameterNameToPath = eventToParameterNameToPath;
        this.defaultParameterNameToPath = defaultParameterNameToPath;
    }


    protected List<String> findPathOrNull(String eventType, String parameterName) {
        Map<String, List<String>> perEvent = eventToParameterNameToPath.get(eventType);
        List<String> path = (perEvent == null) ? null : perEvent.get(parameterName);
        return (path != null) ? path : defaultParameterNameToPath.get(parameterName);
    }


    @Override
    public ErrorsOr<Parameters> parameters(Map<String, Object> input, String eventType, String domainType, String domainId) {
        List<String> parameterValues = new ArrayList<>(parameterNames.size());
        for (String parameterName : parameterNames) {
            List<String> path = findPathOrNull(eventType, parameterName);
            if (path == null)
                return ErrorsOr.error("No path for parameter " + parameterName + " and event type " + eventType);
            String value = ParameterExtractor.findValueOrNull(input, path);
            if (value == null)
                return ErrorsOr.error("No value for parameter " + parameterName + " at path " + path + " and event type " + eventType);
            parameterValues.add(value);
        }
        return ErrorsOr.lift(Parameters.of(parameterNames, parameterValues, domainType, domainId, eventType));
    }
}
