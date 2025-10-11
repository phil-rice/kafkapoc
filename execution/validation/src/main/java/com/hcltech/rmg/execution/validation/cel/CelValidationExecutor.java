package com.hcltech.rmg.execution.validation.cel;


import com.hcltech.rmg.celcore.CelRuleBuilderFactory;
import com.hcltech.rmg.celcore.CelVarType;
import com.hcltech.rmg.celcore.cache.CelRuleCache;
import com.hcltech.rmg.celcore.cache.InMemoryCelRuleCache;
import com.hcltech.rmg.config.config.BehaviorConfig;
import com.hcltech.rmg.config.config.BehaviorConfigVisitor;
import com.hcltech.rmg.config.config.BehaviorConfigWalker;
import com.hcltech.rmg.config.validation.CelValidation;
import com.hcltech.rmg.execution.aspects.AspectExecutor;
import com.hcltech.rmg.messages.ValueEnvelope;

import java.util.*;

public class CelValidationExecutor<CepState, Msg> implements AspectExecutor<CelValidation, ValueEnvelope<CepState, Msg>, List<String>> {


    private final CelRuleCache<ValueEnvelope<CepState, Msg>, List<String>> ruleCache;

    public CelValidationExecutor(CelRuleCache<ValueEnvelope<CepState, Msg>, List<String>> ruleCache) {
        this.ruleCache = Objects.requireNonNull(ruleCache, "CelRuleCache is required");
    }

    @Override
    public List<String> execute(String key, CelValidation celValidation, ValueEnvelope<CepState, Msg> input) {
        return ruleCache.get(key).executor().execute(input);
    }

    public static <CepState, Msg> CelValidationExecutor<CepState, Msg> create(CelRuleBuilderFactory ruleBuilderFactory, BehaviorConfig config) {
        var keyToCel = new HashMap<String, String>();

        BehaviorConfigWalker.walk(config, new BehaviorConfigVisitor() {
            @Override
            public void onCelValidation(String eventName, String moduleName, CelValidation v) {
                String key = BehaviorConfig.configKey(moduleName, BehaviorConfig.validationAspectName, eventName);
                keyToCel.put(key, v.cel());
            }
        });

        var ruleCache = new InMemoryCelRuleCache<>(
                key -> {
                    String source = Objects.requireNonNull(keyToCel.get(key), "No CEL source for key " + key + " Legal values: " + legalKeys(keyToCel));
                    return ruleBuilderFactory.<ValueEnvelope<CepState, Msg>, List<String>>createCelRuleBuilder(source).
                            withVar("msg", CelVarType.DYN, v -> v.data()).
                            withVar("cepState", CelVarType.DYN, v -> v.header().cepState())
                            .compile();
                }, false
        );
        var keys = legalKeys(keyToCel);
        for (String key : keys) {
            ruleCache.populate(key, keyToCel.get(key));
        }

        return new CelValidationExecutor<>(ruleCache);
    }

    private static List<String> legalKeys(HashMap<String, String> keyToCel) {
        ArrayList<String> result = new ArrayList<>(keyToCel.keySet());
        result.sort(String::compareTo);
        return result;
    }
}
