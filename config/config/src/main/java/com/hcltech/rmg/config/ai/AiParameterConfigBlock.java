package com.hcltech.rmg.config.ai;

import com.fasterxml.jackson.annotation.JsonProperty;

/** The object for one parameterKey; contains an "events" section. */
public record AiParameterConfigBlock(
        @JsonProperty("events") AiEvents events
) {}
