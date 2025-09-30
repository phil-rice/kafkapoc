package com.hcltech.rmg.execution.validation.cel;

import com.hcltech.rmg.celcore.RuleBuilderFactory;
import com.hcltech.rmg.celcore.cache.InMemoryRuleCache;
import com.hcltech.rmg.celcore.cache.RuleCache;
import com.hcltech.rmg.common.errorsor.ErrorsOr;
import com.hcltech.rmg.config.config.Config;
import com.hcltech.rmg.config.config.ConfigVisitor;
import com.hcltech.rmg.config.config.ConfigWalker;
import com.hcltech.rmg.config.validation.CelValidation;
import com.hcltech.rmg.execution.aspects.RegisteredAspectExecutor;

import java.util.*;

public class CelValidationExecutor<Inp> implements RegisteredAspectExecutor<CelValidation, Inp, List<String>> {

    private final RuleCache<Inp, List<String>> ruleCache;
    private final Map<String, Object> context;

    public CelValidationExecutor(RuleCache<Inp, List<String>> ruleCache, Map<String, Object> context) {
        this.ruleCache = ruleCache;
        this.context = context;
    }

    @Override
    public ErrorsOr<List<String>> execute(String key, List<String> modules, String aspect, CelValidation celValidation, Inp input) {
        return ruleCache.get(key).flatMap(r -> r.executor().execute(input, context));
    }

    public static <Inp> CelValidationExecutor<Inp> create(RuleBuilderFactory ruleBuilderFactory, Config config) {
        var keyToCel = new HashMap<String, String>();

        ConfigWalker.walk(config, new ConfigVisitor() {
            @Override
            public void onCelValidation(String eventName, String moduleName, CelValidation v) {
                String key = Config.configKey(moduleName, Config.validationAspectName, eventName);
                keyToCel.put(key, v.cel());
            }
        });

        RuleCache<Inp, List<String>> ruleCache =
                new InMemoryRuleCache<Inp, List<String>>(
                        key -> {
                            String source = Objects.requireNonNull(keyToCel.get(key), "No CEL source for key " + key + " Legal values: " + legalKeys(keyToCel));
                            return ruleBuilderFactory.<Inp, List<String>>newRuleBuilder(source).compile();
                        }
                ).preloadWith(legalKeys(keyToCel));

        return new CelValidationExecutor<>(ruleCache, Map.of());
    }

    private static List<String> legalKeys(HashMap<String, String> keyToCel) {
        ArrayList<String> result = new ArrayList<>(keyToCel.keySet());
        result.sort(String::compareTo);
        return result;
    }
}
