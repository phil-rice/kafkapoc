package com.hcltech.rmg.config.configs;

import com.hcltech.rmg.config.bizlogic.BizLogicAspect;
import com.hcltech.rmg.config.bizlogic.CelFileLogic;
import com.hcltech.rmg.config.bizlogic.CelInlineLogic;
import com.hcltech.rmg.config.config.Config;
import com.hcltech.rmg.config.enrich.*;
import com.hcltech.rmg.config.transformation.TransformationAspect;
import com.hcltech.rmg.config.transformation.XmlTransform;
import com.hcltech.rmg.config.transformation.XsltTransform;
import com.hcltech.rmg.config.validation.CelValidation;
import com.hcltech.rmg.config.validation.ValidationAspect;

/**
 * ParamKey-aware visitor for Configs that mirrors BehaviorConfigVisitor callbacks
 * and adds subtype-specific hooks for every EnrichmentAspect.
 */
public interface ConfigsVisitor {
    // Lifecycle for a (paramKey -> Config)
    default void onConfigStart(String paramKey, Config config) {}
    default void onConfigEnd(String paramKey, Config config) {}

    // Validation
    default void onValidation(String paramKey, String eventName, String moduleName, ValidationAspect v) {}
    default void onCelValidation(String paramKey, String eventName, String moduleName, CelValidation v) {}

    // Transformation
    default void onTransformation(String paramKey, String eventName, String moduleName, TransformationAspect t) {}
    default void onXsltTransform(String paramKey, String eventName, String moduleName, XsltTransform t) {}
    default void onXmlTransform(String paramKey, String eventName, String moduleName, XmlTransform t) {}

    // Enrichment (generic + subtype-specific)
    default void onEnrichment(String paramKey, String eventName, String moduleName, EnrichmentAspect e) {}
    default void onMapLookupEnrichment(String paramKey, String eventName, String moduleName, MapLookupEnrichment e) {}
    default void onCompositeEnrichment(String paramKey, String eventName, String moduleName, CompositeExecutor e) {}
    default void onFixedEnrichment(String paramKey, String eventName, String moduleName, FixedEnrichment e) {}
    default void onCsvEnrichment(String paramKey, String eventName, String moduleName, CsvEnrichment e) {}
    default void onCsvAzureEnrichment(String paramKey, String eventName, String moduleName, CsvFromAzureEnrichment e) {}
    default void onApiEnrichment(String paramKey, String eventName, String moduleName, ApiEnrichment e) {}

    // BizLogic
    default void onBizLogic(String paramKey, String eventName, String moduleName, BizLogicAspect b) {}
    default void onCelFileLogic(String paramKey, String eventName, String moduleName, CelFileLogic b) {}
    default void onCelInlineLogic(String paramKey, String eventName, String moduleName, CelInlineLogic b) {}
}
