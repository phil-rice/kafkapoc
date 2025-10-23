package com.hcltech.rmg.config.ai;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import com.hcltech.rmg.config.bizlogic.CelInlineLogic;

import java.util.Map;

/**
 * An event maps aspect name (e.g., "bizlogic", "validation") to
 * a module map { moduleName -> CelInlineLogic }.
 */
public record AiEventDef(@JsonValue Map<String, Map<String, CelInlineLogic>> byAspect) {
    @JsonCreator(mode = JsonCreator.Mode.DELEGATING)
    public AiEventDef {}
}
