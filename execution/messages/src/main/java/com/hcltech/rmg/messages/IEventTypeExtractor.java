package com.hcltech.rmg.messages;

import com.hcltech.rmg.common.Paths;
import com.hcltech.rmg.parameters.ParameterExtractor;

import java.util.List;
import java.util.Map;

public interface IEventTypeExtractor {
    String extractEventType(Map<String, Object> message);

    static final String unknownEventType = "unknown";

    static IEventTypeExtractor fromPath(List<String> path) {
        var copy = List.copyOf(path);
        return message ->
                Paths.findStringOrNull(message, copy);
    }
}
