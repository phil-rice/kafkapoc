package com.hcltech.rmg.config.configs;

import com.hcltech.rmg.config.aspect.AspectMap;
import com.hcltech.rmg.config.bizlogic.BizLogicAspect;
import com.hcltech.rmg.config.bizlogic.CelFileLogic;
import com.hcltech.rmg.config.bizlogic.CelInlineLogic;
import com.hcltech.rmg.config.config.BehaviorConfig;
import com.hcltech.rmg.config.config.Config;
import com.hcltech.rmg.config.configs.Configs;
import com.hcltech.rmg.config.enrich.ApiEnrichment;
import com.hcltech.rmg.config.enrich.EnrichmentAspect;
import com.hcltech.rmg.config.transformation.TransformationAspect;
import com.hcltech.rmg.config.transformation.XmlTransform;
import com.hcltech.rmg.config.transformation.XsltTransform;
import com.hcltech.rmg.config.validation.CelValidation;
import com.hcltech.rmg.config.validation.ValidationAspect;
import com.hcltech.rmg.parameters.ParameterConfig;

/**
 * Visitor for walking {@link Configs}. All hooks take the paramKey first.
 * Mirrors {@link com.hcltech.rmg.config.config.BehaviorConfigVisitor} so it can be adapted seamlessly.
 */
public interface ConfigsVisitor {

    // Top-level
    default void onConfigs(Configs configs) {}
    default void onConfigEntry(String paramKey, Config config) {}
    default void onBehaviorConfig(String paramKey, BehaviorConfig behavior) {}
    default void onParameterConfig(String paramKey, ParameterConfig params) {}
    default void onXmlSchemaPath(String paramKey, String xmlSchemaPath) {}
    default void onEvent(String paramKey, String eventName, AspectMap aspects) {}

    // Validation
    default void onValidation(String paramKey, String eventName, String moduleName, ValidationAspect v) {}
    default void onCelValidation(String paramKey, String eventName, String moduleName, CelValidation v) {}

    // Transformation
    default void onTransformation(String paramKey, String eventName, String moduleName, TransformationAspect t) {}
    default void onXmlTransform(String paramKey, String eventName, String moduleName, XmlTransform t) {}
    default void onXsltTransform(String paramKey, String eventName, String moduleName, XsltTransform t) {}

    // Enrichment
    default void onEnrichment(String paramKey, String eventName, String moduleName, EnrichmentAspect e) {}
    default void onApiEnrichment(String paramKey, String eventName, String moduleName, ApiEnrichment e) {}

    // BizLogic
    default void onBizLogic(String paramKey, String eventName, String moduleName, BizLogicAspect b) {}
    default void onCelFileLogic(String paramKey, String eventName, String moduleName, CelFileLogic b) {}
    default void onCelInlineLogic(String paramKey, String eventName, String moduleName, CelInlineLogic b) {}
}
