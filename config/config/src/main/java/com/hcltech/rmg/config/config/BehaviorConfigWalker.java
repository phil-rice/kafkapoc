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

import java.util.Map;
import java.util.Objects;

/**
 * Depth-first walker over the configuration graph starting at {@link BehaviorConfig}.
 *
 * <p>Traversal order per event:
 * <ol>
 *   <li>validation</li>
 *   <li>transformation</li>
 *   <li>enrichment</li>
 *   <li>bizlogic</li>
 * </ol>
 */
public final class BehaviorConfigWalker {

    private BehaviorConfigWalker() {
    }

    public static void walk(BehaviorConfig config, BehaviorConfigVisitor visitor) {
        Objects.requireNonNull(config, "config");
        Objects.requireNonNull(visitor, "visitor");

        visitor.onConfig(config);

        for (Map.Entry<String, AspectMap> evt : config.events().entrySet()) {
            final String eventName = evt.getKey();
            final AspectMap aspects = evt.getValue();
            if (aspects == null) continue;

            visitor.onEvent(eventName, aspects);

            // --- Validation ---
            if (aspects.validation() != null) {
                for (Map.Entry<String, ValidationAspect> e : aspects.validation().entrySet()) {
                    String name = e.getKey();
                    ValidationAspect v = e.getValue();
                    if (v == null) continue;

                    visitor.onValidation(eventName, name, v);
                    if (v instanceof CelValidation xsv)
                        visitor.onCelValidation(eventName, name, xsv);
                }
            }

            // --- Transformation ---
            if (aspects.transformation() != null) {
                for (Map.Entry<String, TransformationAspect> e : aspects.transformation().entrySet()) {
                    String name = e.getKey();
                    TransformationAspect t = e.getValue();
                    if (t == null) continue;

                    visitor.onTransformation(eventName, name, t);
                    if (t instanceof XsltTransform xslt)
                        visitor.onXsltTransform(eventName, name, xslt);
                }
            }

            // --- Enrichment ---
            if (aspects.enrichment() != null) {
                for (Map.Entry<String, EnrichmentAspect> e : aspects.enrichment().entrySet()) {
                    String name = e.getKey();
                    EnrichmentAspect enr = e.getValue();
                    if (enr == null) continue;

                    visitor.onEnrichment(eventName, name, enr);
                    if (enr instanceof ApiEnrichment api)
                        visitor.onApiEnrichment(eventName, name, api);
                }
            }

            // --- BizLogic ---
            if (aspects.bizlogic() != null) {
                for (Map.Entry<String, BizLogicAspect> e : aspects.bizlogic().entrySet()) {
                    String name = e.getKey();
                    BizLogicAspect b = e.getValue();
                    if (b == null) continue;

                    visitor.onBizLogic(eventName, name, b);
                    if (b instanceof CelFileLogic file)
                        visitor.onCelFileLogic(eventName, name, file);
                    else if (b instanceof CelInlineLogic inline)
                        visitor.onCelInlineLogic(eventName, name, inline);
                }
            }
        }
    }

}
