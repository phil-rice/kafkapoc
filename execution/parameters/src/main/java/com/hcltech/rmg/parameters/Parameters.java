package com.hcltech.rmg.parameters;

import java.util.List;
import java.util.function.Function;

/**
 * This holds the parameters in the current message being processed.
 *
 * @param parameterNames  The names of the parameters
 * @param parameterValues The values of the parameters. They are in the same order as the names
 * @param key             The key formed by concatenating the parameter values
 *
 */
public record Parameters(List<String> parameterNames, List<String> parameterValues, String key,
                         String domainType,
                         String domainId,
                         String eventType) {
    public static String defaultKeyFn(List<String> list) {
        return String.join(":", list);
    }

    public static Function<List<String>, String> defaultResourceFn(String prefix) {
        return (list) -> prefix + String.join("/", list) +".json";
    }

    // Canonical constructor with validation
    public Parameters {
        if (parameterNames.size() != parameterValues.size()) {
            throw new IllegalArgumentException("Parameter names and values must have the same size");
        }
    }


    public static Parameters of(List<String> parameterNames, List<String> parameterValues, String domainType, String domainId, String eventType) {
        return new Parameters(parameterNames, parameterValues, defaultKeyFn(parameterValues), domainType, domainId, eventType);
    }
}
