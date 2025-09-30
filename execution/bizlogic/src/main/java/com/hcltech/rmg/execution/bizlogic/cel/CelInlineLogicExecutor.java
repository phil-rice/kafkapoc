package com.hcltech.rmg.execution.bizlogic.cel;

import com.hcltech.rmg.celcore.RuleBuilderFactory;
import com.hcltech.rmg.celcore.cache.InMemoryRuleCache;
import com.hcltech.rmg.celcore.cache.RuleCache;
import com.hcltech.rmg.common.errorsor.ErrorsOr;
import com.hcltech.rmg.config.bizlogic.CelInlineLogic;
import com.hcltech.rmg.config.config.Config;
import com.hcltech.rmg.config.config.ConfigVisitor;
import com.hcltech.rmg.config.config.ConfigWalker;
import com.hcltech.rmg.execution.aspects.RegisteredAspectExecutor;

import java.util.*;

public class CelInlineLogicExecutor<Inp> implements RegisteredAspectExecutor<CelInlineLogic, Inp, List<String>> {

    private final RuleCache<Inp, List<String>> ruleCache;
    private final Map<String, Object> context;

    public CelInlineLogicExecutor(RuleCache<Inp, List<String>> ruleCache, Map<String, Object> context) {
        this.ruleCache = ruleCache;
        this.context = context;
    }

    @Override
    public ErrorsOr<List<String>> execute(String key, List<String> modules, String aspect, CelInlineLogic celInlineLogic, Inp input) {
        return ruleCache.get(key).flatMap(r -> r.executor().execute(input, context));
    }

    public static <Inp> CelInlineLogicExecutor<Inp> create(RuleBuilderFactory ruleBuilderFactory, Config config) {
        var keyToCel = new HashMap<String, String>();

        ConfigWalker.walk(config, new ConfigVisitor() {
            @Override
            public void onCelInlineLogic(String eventName, String moduleName, CelInlineLogic b) {
                    String key = Config.configKey(moduleName, Config.bizlogicAspectName, eventName);
                keyToCel.put(key, b.cel());
            }

        });

        RuleCache<Inp, List<String>> ruleCache =
                new InMemoryRuleCache<Inp, List<String>>(
                        key -> {
                            String source = Objects.requireNonNull(keyToCel.get(key), "No CEL source for key " + key + " Legal values: " + legalKeys(keyToCel));
                            return ruleBuilderFactory.<Inp, List<String>>newRuleBuilder(source).compile();
                        }
                ).preloadWith(legalKeys(keyToCel));

        return new CelInlineLogicExecutor<>(ruleCache, Map.of());
    }

    private static List<String> legalKeys(Map<String, String> keyToCel) {
        ArrayList<String> result = new ArrayList<>(keyToCel.keySet());
        result.sort(String::compareTo);
        return result;
    }
}
