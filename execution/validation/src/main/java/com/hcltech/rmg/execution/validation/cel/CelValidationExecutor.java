// CelValidationExecutor.java
package com.hcltech.rmg.execution.validation.cel;

import com.hcltech.rmg.celcore.CelRuleBuilderFactory;
import com.hcltech.rmg.celcore.CelVarType;
import com.hcltech.rmg.celcore.cache.CelRuleCache;
import com.hcltech.rmg.celcore.cache.InMemoryCelRuleCache;
import com.hcltech.rmg.config.config.BehaviorConfig;
import com.hcltech.rmg.config.config.BehaviorConfigVisitor;
import com.hcltech.rmg.config.config.BehaviorConfigWalker;
import com.hcltech.rmg.config.validation.CelValidation;
import com.hcltech.rmg.execution.aspects.AspectExecutorSync;
import com.hcltech.rmg.messages.ValueEnvelope;

import java.util.*;

public class CelValidationExecutor<CepState, Msg>
        implements AspectExecutorSync<CelValidation, ValueEnvelope<CepState, Msg>, List<String>> {

    private final CelRuleCache<ValueEnvelope<CepState, Msg>, List<String>> ruleCache;

    public CelValidationExecutor(CelRuleCache<ValueEnvelope<CepState, Msg>, List<String>> ruleCache) {
        this.ruleCache = Objects.requireNonNull(ruleCache, "CelRuleCache is required");
    }

    @Override
    public List<String> execute(String key, CelValidation celValidation, ValueEnvelope<CepState, Msg> input) {
        return ruleCache.get(key).executor().execute(input);
    }

    public static <CepState, Msg> CelValidationExecutor<CepState, Msg> create(
            CelRuleBuilderFactory ruleBuilderFactory, BehaviorConfig config) {

        var keyToCel = new HashMap<String, String>();

        BehaviorConfigWalker.walk(config, new BehaviorConfigVisitor() {
            @Override
            public void onCelValidation(String eventName, String moduleName, CelValidation v) {
                String key = BehaviorConfig.configKey(moduleName, BehaviorConfig.validationAspectName, eventName);
                keyToCel.put(key, v.cel());
            }
        });

        // compileFn parameter is the CEL SOURCE (not the key)
        var ruleCache = new InMemoryCelRuleCache<>(
                source -> ruleBuilderFactory
                        .<ValueEnvelope<CepState, Msg>, List<String>>createCelRuleBuilder(source)
                        .withVar("msg", CelVarType.DYN, ValueEnvelope::data)
                        .withVar("cepState", CelVarType.DYN, ValueEnvelope::cepState)
                        .compile(),
                /* overwriteOnPopulate */ false
        );

        // Preload deterministically
        var keys = legalKeys(keyToCel);
        for (String key : keys) {
            String source = Objects.requireNonNull(
                    keyToCel.get(key),
                    "No CEL source for key " + key + " Legal values: " + keys
            );
            ruleCache.populate(key, source);
        }

        return new CelValidationExecutor<>(ruleCache);
    }

    private static List<String> legalKeys(Map<String, String> keyToCel) {
        ArrayList<String> result = new ArrayList<>(keyToCel.keySet());
        result.sort(String::compareTo);
        return result;
    }
}
