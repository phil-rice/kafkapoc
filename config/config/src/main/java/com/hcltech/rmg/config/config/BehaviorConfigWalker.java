package com.hcltech.rmg.config.config;

import com.hcltech.rmg.config.bizlogic.CelFileLogic;
import com.hcltech.rmg.config.bizlogic.CelInlineLogic;
import com.hcltech.rmg.config.enrich.FixedEnrichment;
import com.hcltech.rmg.config.transformation.XmlTransform;
import com.hcltech.rmg.config.transformation.XsltTransform;
import com.hcltech.rmg.config.validation.CelValidation;

import java.util.Map;
import java.util.Objects;
import java.util.function.BiConsumer;

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

        forEachNonNull(config.events(), (eventName, aspects) -> {
            visitor.onEvent(eventName, aspects);

            // --- Validation ---
            forEachNonNull(aspects.validation(), (moduleName, v) -> {
                visitor.onValidation(eventName, moduleName, v);
                if (v instanceof CelValidation cv) {
                    visitor.onCelValidation(eventName, moduleName, cv);
                }
            });

            // --- Transformation ---
            forEachNonNull(aspects.transformation(), (moduleName, t) -> {
                visitor.onTransformation(eventName, moduleName, t);
                if (t instanceof XmlTransform xt) {
                    visitor.onXmlTransform(eventName, moduleName, xt);
                } else if (t instanceof XsltTransform xslt) {
                    visitor.onXsltTransform(eventName, moduleName, xslt);
                }
            });

            // --- Enrichment ---
            forEachNonNull(aspects.enrichment(), (moduleName, e) -> {
                visitor.onEnrichment(eventName, moduleName, e);
                if (e instanceof FixedEnrichment fixed) {   // <-- NEW
                    visitor.onFixedEnrichment(eventName, moduleName, fixed);
                }
            });

            // --- BizLogic ---
            forEachNonNull(aspects.bizlogic(), (moduleName, b) -> {
                visitor.onBizLogic(eventName, moduleName, b);
                if (b instanceof CelFileLogic file) {
                    visitor.onCelFileLogic(eventName, moduleName, file);
                } else if (b instanceof CelInlineLogic inline) {
                    visitor.onCelInlineLogic(eventName, moduleName, inline);
                }
            });
        });
    }

    private static <K, V> void forEachNonNull(Map<K, V> map, BiConsumer<K, V> consumer) {
        if (map == null || map.isEmpty()) return;
        map.forEach((k, v) -> {
            if (v != null) consumer.accept(k, v);
        });
    }
}
