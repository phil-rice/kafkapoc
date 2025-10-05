package com.hcltech.rmg.messages;

import com.hcltech.rmg.parameters.ParameterExtractor;

import java.util.List;
import java.util.Map;

public interface IDomainTypeExtractor {
    String extractDomainType(Map<String, Object> message);

    static IDomainTypeExtractor fromPath(List<String> path) {
        var copy = List.copyOf(path);
        return message -> ParameterExtractor.findValueOrNull(message, copy);
    }

    static IDomainTypeExtractor fixed(String domainType) {
        return message -> domainType;
    }
}
