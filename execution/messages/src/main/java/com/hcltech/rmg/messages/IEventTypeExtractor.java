package com.hcltech.rmg.messages;

import com.hcltech.rmg.common.Paths;

import java.util.List;
import java.util.Map;

public interface IEventTypeExtractor<Msg> {
    String extractEventType(Msg message);

    static final String unknownEventType = "unknown";

    static IEventTypeExtractor<Map<String, Object>> fromPathForMapStringObject(List<String> path) {
        var copy = List.copyOf(path);
        return message ->
                Paths.findStringOrNull(message, copy);
    }
}
