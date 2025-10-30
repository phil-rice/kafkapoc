package com.hcltech.rmg.messages;

import com.hcltech.rmg.common.Paths;
import com.hcltech.rmg.parameters.ParameterExtractor;

import java.util.List;
import java.util.Map;

public interface IDomainTypeExtractor<Msg> {
    String extractDomainType(Msg message);

    static IDomainTypeExtractor<Map<String, Object>> fromPath(List<String> path) {
        var copy = List.copyOf(path);
        return message -> Paths.findStringOrNull(message, copy);
    }

    static <Msg> IDomainTypeExtractor<Msg> fixed(String domainType) {
        return message -> domainType;
    }
}
