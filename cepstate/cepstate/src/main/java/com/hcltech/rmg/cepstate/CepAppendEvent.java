package com.hcltech.rmg.cepstate;

import com.fasterxml.jackson.annotation.JsonTypeName;
import com.hcltech.rmg.common.Paths;

import java.util.List;
import java.util.Map;
@JsonTypeName("append")
public record CepAppendEvent(List<String> path, Object value) implements CepEvent {
    @Override
    public Map<String, Object> fold(Map<String, Object> state) {
        return Paths.appendValue(state, path, value);
    }
}
