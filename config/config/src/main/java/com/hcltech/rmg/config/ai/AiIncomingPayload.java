package com.hcltech.rmg.config.ai;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.hcltech.rmg.config.config.RootConfig;

/** Root wrapper for the incoming AI-specific configuration payload. */
public record AiIncomingPayload(
        @JsonProperty("config") AiParameterKeyConfigs config,
        @JsonProperty("cel") String celProjection,
        @JsonProperty("rootConfig")RootConfig rootConfig
) {}
