package com.hcltech.rmg.config.config;


import com.hcltech.rmg.parameters.ParameterConfig;

public record Config(BehaviorConfig behaviorConfig, ParameterConfig parameterConfig, String xmlSchemaPath) {
}
