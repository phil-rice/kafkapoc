package com.hcltech.rmg.config.config;

import com.hcltech.rmg.parameters.ParameterConfig;

/**
 * The root/shared config file:
 * - parameterConfig: defines/declares the parameter space and defaults
 * - xmlSchemaPath:   shared XSD path
 */
public record RootConfig(ParameterConfig parameterConfig, String xmlSchemaPath) { }
