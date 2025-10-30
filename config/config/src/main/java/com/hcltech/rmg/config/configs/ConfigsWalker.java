package com.hcltech.rmg.config.configs;

import com.hcltech.rmg.config.aspect.AspectMap;
import com.hcltech.rmg.config.bizlogic.BizLogicAspect;
import com.hcltech.rmg.config.bizlogic.CelFileLogic;
import com.hcltech.rmg.config.bizlogic.CelInlineLogic;
import com.hcltech.rmg.config.config.BehaviorConfig;
import com.hcltech.rmg.config.config.BehaviorConfigVisitor;
import com.hcltech.rmg.config.config.BehaviorConfigWalker;
import com.hcltech.rmg.config.config.Config;
import com.hcltech.rmg.config.enrich.EnrichmentAspect;
import com.hcltech.rmg.config.transformation.TransformationAspect;
import com.hcltech.rmg.config.transformation.XmlTransform;
import com.hcltech.rmg.config.transformation.XsltTransform;
import com.hcltech.rmg.config.validation.CelValidation;
import com.hcltech.rmg.config.validation.ValidationAspect;

import java.util.Map;
import java.util.Objects;

/**
 * Walker over {@link Configs}. For each (paramKey, Config), invokes
 * ConfigsVisitor hooks and delegates into {@link BehaviorConfigWalker}
 * via an adapter that injects paramKey into every callback.
 */
public final class ConfigsWalker {

    private ConfigsWalker() {}

    public static void walk(Configs configs, ConfigsVisitor visitor) {
        Objects.requireNonNull(configs, "configs");
        Objects.requireNonNull(visitor, "visitor");

        visitor.onConfigs(configs);

        for (Map.Entry<String, Config> e : configs.keyToConfigMap().entrySet()) {
            final String paramKey = e.getKey();
            final Config cfg = e.getValue();
            if (cfg == null) continue;

            visitor.onConfigEntry(paramKey, cfg);

            // Optional: surface parameterConfig and xmlSchemaPath at this level
            if (cfg.parameterConfig() != null) {
                visitor.onParameterConfig(paramKey, cfg.parameterConfig());
            }
            if (cfg.xmlSchemaPath() != null) {
                visitor.onXmlSchemaPath(paramKey, cfg.xmlSchemaPath());
            }

            final BehaviorConfig behavior = cfg.behaviorConfig();
            if (behavior == null) continue;

            visitor.onBehaviorConfig(paramKey, behavior);

            // Delegate into BehaviorConfigWalker with a paramKey-aware adapter
            BehaviorConfigWalker.walk(behavior, new BehaviorToConfigsAdapter(paramKey, visitor));
        }
    }

    /**
     * Adapts BehaviorConfigVisitor calls to ConfigsVisitor, injecting paramKey.
     */
    private static final class BehaviorToConfigsAdapter implements BehaviorConfigVisitor {
        private final String paramKey;
        private final ConfigsVisitor v;

        BehaviorToConfigsAdapter(String paramKey, ConfigsVisitor v) {
            this.paramKey = paramKey;
            this.v = v;
        }

        @Override public void onConfig(BehaviorConfig config) {
            // no-op: already surfaced via onBehaviorConfig at the configs level
        }

        @Override public void onEvent(String eventName, AspectMap aspects) {
            v.onEvent(paramKey, eventName, aspects);
        }

        // Validation
        @Override public void onValidation(String eventName, String moduleName, ValidationAspect val) {
            v.onValidation(paramKey, eventName, moduleName, val);
        }
        @Override public void onCelValidation(String eventName, String moduleName, CelValidation val) {
            v.onCelValidation(paramKey, eventName, moduleName, val);
        }

        // Transformation
        @Override public void onTransformation(String eventName, String moduleName, TransformationAspect t) {
            v.onTransformation(paramKey, eventName, moduleName, t);
        }
        @Override public void onXmlTransform(String eventName, String moduleName, XmlTransform t) {
            v.onXmlTransform(paramKey, eventName, moduleName, t);
        }
        @Override public void onXsltTransform(String eventName, String moduleName, XsltTransform t) {
            v.onXsltTransform(paramKey, eventName, moduleName, t);
        }

        // Enrichment
        @Override public void onEnrichment(String eventName, String moduleName, EnrichmentAspect e) {
            v.onEnrichment(paramKey, eventName, moduleName, e);
        }


        // BizLogic
        @Override public void onBizLogic(String eventName, String moduleName, BizLogicAspect b) {
            v.onBizLogic(paramKey, eventName, moduleName, b);
        }
        @Override public void onCelFileLogic(String eventName, String moduleName, CelFileLogic b) {
            v.onCelFileLogic(paramKey, eventName, moduleName, b);
        }
        @Override public void onCelInlineLogic(String eventName, String moduleName, CelInlineLogic b) {
            v.onCelInlineLogic(paramKey, eventName, moduleName, b);
        }
    }
}
