package com.hcltech.rmg.config.ai;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

import java.util.Map;

/** "config": dynamic parameterKey -> AiParameterConfigBlock */
public record AiParameterKeyConfigs(@JsonValue Map<String, AiParameterConfigBlock> byParameterKey) {
    @JsonCreator(mode = JsonCreator.Mode.DELEGATING)
    public AiParameterKeyConfigs {}
}
