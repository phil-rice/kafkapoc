package com.hcltech.rmg.config.ai;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

import java.util.Map;

/** "events": dynamic eventName -> AiEventDef */
public record AiEvents(@JsonValue Map<String, AiEventDef> byEvent) {
    @JsonCreator(mode = JsonCreator.Mode.DELEGATING)
    public AiEvents {}
}
