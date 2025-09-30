// File: src/main/java/com/hcltech/rmg/config/walker/ConfigVisitor.java
package com.hcltech.rmg.config.config;

import com.hcltech.rmg.config.aspect.AspectMap;
import com.hcltech.rmg.config.bizlogic.BizLogicAspect;
import com.hcltech.rmg.config.bizlogic.CelFileLogic;
import com.hcltech.rmg.config.bizlogic.CelInlineLogic;
import com.hcltech.rmg.config.enrich.ApiEnrichment;
import com.hcltech.rmg.config.enrich.EnrichmentAspect;
import com.hcltech.rmg.config.transformation.TransformationAspect;
import com.hcltech.rmg.config.transformation.XsltTransform;
import com.hcltech.rmg.config.validation.CelValidation;
import com.hcltech.rmg.config.validation.ValidationAspect;

/**
 * Visitor with a hook for every node type in the configuration object graph.
 *
 * <p>There are two styles of hooks:
 * <ul>
 *   <li>Generic hooks for each interface (e.g. {@code onValidation(...)})</li>
 *   <li>Specific hooks for concrete types (e.g. {@code onXmlSchemaValidation(...)})</li>
 * </ul>
 * The walker will always invoke the more general hook first, followed by the
 * specific hook when it can determine the concrete subtype.
 */
public interface ConfigVisitor {
    // Root & container nodes
    default void onConfig(Config config) {}
    default void onEvent(String eventName, AspectMap aspects) {}

    // Validation family
    default void onValidation(String eventName, String moduleName, ValidationAspect v) {}
    default void onCelValidation(String eventName, String moduleName, CelValidation v) {}

    // Transformation family
    default void onTransformation(String eventName, String moduleName, TransformationAspect t) {}
    default void onXsltTransform(String eventName, String moduleName, XsltTransform t) {}

    // Enrichment family
    default void onEnrichment(String eventName, String moduleName, EnrichmentAspect e) {}
    default void onApiEnrichment(String eventName, String moduleName, ApiEnrichment e) {}

    // BizLogic family
    default void onBizLogic(String eventName, String moduleName, BizLogicAspect b) {}
    default void onCelFileLogic(String eventName, String moduleName, CelFileLogic b) {}
    default void onCelInlineLogic(String eventName, String moduleName, CelInlineLogic b) {}

}