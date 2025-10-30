package com.hcltech.rmg.config.config;


import com.hcltech.rmg.parameters.ParameterConfig;

import java.io.Serializable;

public record Config(BehaviorConfig behaviorConfig, ParameterConfig parameterConfig, String xmlSchemaPath,
                     String celForAi) implements Serializable {
    public Config(BehaviorConfig behaviorConfig, ParameterConfig parameterConfig, String xmlSchemaPath) {
        this(behaviorConfig, parameterConfig, xmlSchemaPath, null);
    }
}
