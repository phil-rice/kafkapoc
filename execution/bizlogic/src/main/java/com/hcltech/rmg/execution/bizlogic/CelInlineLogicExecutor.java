// CelInlineLogicExecutor.java
package com.hcltech.rmg.execution.bizlogic;

import com.hcltech.rmg.celcore.CelExecutor;
import com.hcltech.rmg.celcore.CelRuleBuilderFactory;
import com.hcltech.rmg.celcore.CelVarType;
import com.hcltech.rmg.celcore.CompiledCelRule;
import com.hcltech.rmg.celcore.cache.CelRuleCache;
import com.hcltech.rmg.celcore.cache.InMemoryCelRuleCache;
import com.hcltech.rmg.config.bizlogic.CelInlineLogic;
import com.hcltech.rmg.config.config.BehaviorConfig;
import com.hcltech.rmg.config.configs.Configs;
import com.hcltech.rmg.config.configs.ConfigsVisitor;
import com.hcltech.rmg.config.configs.ConfigsWalker;
import com.hcltech.rmg.execution.aspects.AspectExecutor;
import com.hcltech.rmg.messages.ValueEnvelope;

import java.sql.Array;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public record CelInlineLogicExecutor<CepState, Msg>(
        CelRuleCache<ValueEnvelope<CepState, Msg>, ValueEnvelope<CepState, Msg>> ruleCache,
        Map<String, String> keyToCel)
        implements AspectExecutor<CelInlineLogic, ValueEnvelope<CepState, Msg>, ValueEnvelope<CepState, Msg>> {

    public CelInlineLogicExecutor(CelRuleCache<ValueEnvelope<CepState, Msg>, ValueEnvelope<CepState, Msg>> ruleCache, Map<String, String> keyToCel) {
        this.ruleCache = Objects.requireNonNull(ruleCache, "CelRuleCache is required");
        this.keyToCel = keyToCel;
    }

    @Override
    public ValueEnvelope<CepState, Msg> execute(String key,
                                                CelInlineLogic celInlineLogic,
                                                ValueEnvelope<CepState, Msg> input) {
        var rule = ruleCache.get(key);
        var executor = rule.executor();
        var result = executor.execute(input);
        return result;
    }

    /**
     * Require the message class for safe, contextual coercion.
     */
    public static <CepState, Msg> CelInlineLogicExecutor<CepState, Msg> create(
            CelRuleBuilderFactory ruleBuilderFactory,
            Configs configs,                    // <— now Configs, not BehaviorConfig
            Class<Msg> msgClass) {

        var keyToCel = new java.util.HashMap<String, String>();

        // Walk all configs; collect inline CEL by (paramKey,event,module)
        ConfigsWalker.walk(configs, new ConfigsVisitor() {
            @Override
            public void onCelInlineLogic(String paramKey, String eventName, String moduleName, com.hcltech.rmg.config.bizlogic.CelInlineLogic b) {
                // Centralize how keys are made so you can swap in your final form later.
                String key = Configs.composeKey(paramKey, eventName, moduleName);
                keyToCel.put(key, b.cel());
            }
        });

        // compileFn parameter is the CEL SOURCE (not the key)
        InMemoryCelRuleCache<ValueEnvelope<CepState, Msg>, ValueEnvelope<CepState, Msg>> ruleCache =
                new InMemoryCelRuleCache<>(
                        source -> ruleBuilderFactory
                                .<ValueEnvelope<CepState, Msg>, ValueEnvelope<CepState, Msg>>createCelRuleBuilder(source)
                                .withVar("message", CelVarType.DYN, ValueEnvelope::data)
                                .withVar("cepState", CelVarType.DYN, v ->
                                        java.util.Objects.requireNonNull(v.cepState(), "ValueEnvelope.cepState() is null"))
                                .withResultCoercer((ve, out) -> {
                                    if (out == null) {
                                        // Policy: keep original data if rule yields null
                                        return ve;
                                    }
                                    if (!msgClass.isInstance(out)) {
                                        throw new IllegalArgumentException(
                                                "CEL result type does not match expected message type. "
                                                        + "Expected: " + msgClass.getName()
                                                        + ", got: " + out.getClass().getName());
                                    }
                                    return ve.withData(msgClass.cast(out));
                                })
                                .compile(),
                        /* overwriteOnPopulate */ false
                );

        // Preload deterministically
        var keys = legalKeys(keyToCel);
var allCelErrors = new ArrayList<>();
        for (String key : keys) {
            String source = java.util.Objects.requireNonNull(
                    keyToCel.get(key),
                    "No CEL source for key " + key + " Legal values: " + keys
            );
            var res = ruleCache.populate(key, source);
            if (res.isError())
                allCelErrors.addAll(res.addPrefixIfError("Error compiling CEL for key " + key + ": ").errorsOrThrow());
        }
        if (allCelErrors.size()>0)
            throw new InvalidCelException("Errors compiling CEL inline logic: " + allCelErrors, allCelErrors);
        return new CelInlineLogicExecutor<>(ruleCache, keyToCel);
    }

    /**
     * Compose the cache key for inline CEL rules.
     * Replace this with your "correct form" once decided.
     * <p>
     * Current default: include paramKey + existing behavior key shape.
     */

    private static java.util.List<String> legalKeys(java.util.Map<String, String> keyToCel) {
        var result = new java.util.ArrayList<String>(keyToCel.keySet());
        result.sort(String::compareTo);
        return result;
    }

}
