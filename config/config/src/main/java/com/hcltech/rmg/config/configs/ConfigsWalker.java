package com.hcltech.rmg.config.configs;

import com.hcltech.rmg.config.bizlogic.BizLogicAspect;
import com.hcltech.rmg.config.bizlogic.CelFileLogic;
import com.hcltech.rmg.config.bizlogic.CelInlineLogic;
import com.hcltech.rmg.config.config.BehaviorConfig;
import com.hcltech.rmg.config.config.BehaviorConfigVisitor;
import com.hcltech.rmg.config.config.BehaviorConfigWalker;
import com.hcltech.rmg.config.config.Config;
import com.hcltech.rmg.config.enrich.*;
import com.hcltech.rmg.config.transformation.TransformationAspect;
import com.hcltech.rmg.config.transformation.XmlTransform;
import com.hcltech.rmg.config.transformation.XsltTransform;
import com.hcltech.rmg.config.validation.CelValidation;
import com.hcltech.rmg.config.validation.ValidationAspect;

import java.util.Comparator;

/**
 * Walks (paramKey -> Config) entries and forwards callbacks to a ConfigsVisitor,
 * injecting paramKey. Inner ordering is delegated to BehaviorConfigWalker.
 * Param keys are visited in deterministic (sorted) order.
 */
public final class ConfigsWalker {
    private ConfigsWalker() {}

    public static void walk(Configs configs, ConfigsVisitor visitor) {
        if (configs == null || configs.keyToConfigMap() == null || configs.keyToConfigMap().isEmpty()) return;

        configs.keyToConfigMap().entrySet().stream()
                .sorted(Comparator.comparing(e -> e.getKey() == null ? "" : e.getKey()))
                .forEach(entry -> {
                    final String paramKey = entry.getKey();
                    final Config config = entry.getValue();
                    if (config == null) return;

                    visitor.onConfigStart(paramKey, config);

                    final BehaviorConfig behavior = config.behaviorConfig();
                    if (behavior != null) {
                        BehaviorConfigWalker.walk(behavior, new ParamKeyForwardingVisitor(paramKey, visitor));
                    }

                    visitor.onConfigEnd(paramKey, config);
                });
    }

    /**
     * Adapts BehaviorConfigVisitor -> ConfigsVisitor, adding paramKey.
     * We keep subtype dispatch for EnrichmentAspect here so BehaviorConfigVisitor
     * does not need to be extended.
     */
    private static final class ParamKeyForwardingVisitor implements BehaviorConfigVisitor {
        private final String paramKey;
        private final ConfigsVisitor v;

        private ParamKeyForwardingVisitor(String paramKey, ConfigsVisitor v) {
            this.paramKey = paramKey;
            this.v = v;
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
        @Override public void onXsltTransform(String eventName, String moduleName, XsltTransform t) {
            v.onXsltTransform(paramKey, eventName, moduleName, t);
        }
        @Override public void onXmlTransform(String eventName, String moduleName, XmlTransform t) {
            v.onXmlTransform(paramKey, eventName, moduleName, t);
        }

        // Enrichment (generic + subtype-specific dispatch)
        @Override public void onEnrichment(String eventName, String moduleName, EnrichmentAspect e) {
            v.onEnrichment(paramKey, eventName, moduleName, e);

            if (e instanceof MapLookupEnrichment m) {
                v.onMapLookupEnrichment(paramKey, eventName, moduleName, m);
            } else if (e instanceof CompositeExecutor c) {
                v.onCompositeEnrichment(paramKey, eventName, moduleName, c);
            } else if (e instanceof FixedEnrichment f) {
                v.onFixedEnrichment(paramKey, eventName, moduleName, f);
            } else if (e instanceof CsvEnrichment csv) {
                v.onCsvEnrichment(paramKey, eventName, moduleName, csv);
            } else if (e instanceof CsvFromAzureEnrichment csvAz) {
                v.onCsvAzureEnrichment(paramKey, eventName, moduleName, csvAz);
            } else if (e instanceof ApiEnrichment api) {
                v.onApiEnrichment(paramKey, eventName, moduleName, api);
            }
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
