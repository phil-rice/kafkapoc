// CelInlineLogicExecutor.java
package com.hcltech.rmg.execution.bizlogic;

import com.hcltech.rmg.celcore.CelRuleBuilderFactory;
import com.hcltech.rmg.celcore.CelVarType;
import com.hcltech.rmg.celcore.cache.CelRuleCache;
import com.hcltech.rmg.celcore.cache.InMemoryCelRuleCache;
import com.hcltech.rmg.config.bizlogic.CelInlineLogic;
import com.hcltech.rmg.config.configs.Configs;
import com.hcltech.rmg.config.configs.ConfigsVisitor;
import com.hcltech.rmg.config.configs.ConfigsWalker;
import com.hcltech.rmg.execution.aspects.AspectExecutorAsync;
import com.hcltech.rmg.messages.ValueEnvelope;
import com.hcltech.rmg.common.function.Callback;

import java.util.Map;
import java.util.Objects;

/**
 * Async-shaped executor for inline CEL business logic.
 * Currently synchronous: invokes the callback inline with the result.
 * If CEL evaluation ever becomes asynchronous, keep the interface and call cb later.
 */
public record CelInlineLogicExecutor<CepState, Msg>(
        CelRuleCache<ValueEnvelope<CepState, Msg>, ValueEnvelope<CepState, Msg>> ruleCache,
        Map<String, String> keyToCel
) implements AspectExecutorAsync<CelInlineLogic,
        ValueEnvelope<CepState, Msg>,
        ValueEnvelope<CepState, Msg>> {

    public CelInlineLogicExecutor(
            CelRuleCache<ValueEnvelope<CepState, Msg>, ValueEnvelope<CepState, Msg>> ruleCache,
            Map<String, String> keyToCel) {
        this.ruleCache = Objects.requireNonNull(ruleCache, "CelRuleCache is required");
        this.keyToCel  = Objects.requireNonNull(keyToCel,  "keyToCel map is required");
    }

    @Override
    public void call(String key,
                     CelInlineLogic aspect,
                     ValueEnvelope<CepState, Msg> input,
                     Callback<? super ValueEnvelope<CepState, Msg>> cb) {
        try {
            var rule = ruleCache.get(key);           // key provided by dispatcher (Configs.composeKey(...)
            var result = rule.executor().execute(input);
            cb.success(result);
        } catch (Throwable t) {
            cb.failure(t);
        }
    }

    /**
     * Factory: compiles and preloads all inline CEL rules from {@link Configs}.
     * Requires the message class for safe coercion of CEL outputs.
     */
    public static <CepState, Msg> CelInlineLogicExecutor<CepState, Msg> create(
            CelRuleBuilderFactory ruleBuilderFactory,
            Configs configs,
            Class<Msg> msgClass) {

        var keyToCel = new java.util.HashMap<String, String>();

        // Walk all configs; collect inline CEL by (paramKey,event,module)
        ConfigsWalker.walk(configs, new ConfigsVisitor() {
            @Override
            public void onCelInlineLogic(String paramKey, String eventName, String moduleName,
                                         com.hcltech.rmg.config.bizlogic.CelInlineLogic b) {
                String key = Configs.composeKey(paramKey, eventName, moduleName);
                keyToCel.put(key, b.cel());
            }
        });

        // compileFn consumes CEL SOURCE (value in keyToCel), keyed by our composed key.
        InMemoryCelRuleCache<ValueEnvelope<CepState, Msg>, ValueEnvelope<CepState, Msg>> ruleCache =
                new InMemoryCelRuleCache<>(
                        source -> ruleBuilderFactory
                                .<ValueEnvelope<CepState, Msg>, ValueEnvelope<CepState, Msg>>createCelRuleBuilder(source)
                                // Expose variables
                                .withVar("message",  CelVarType.DYN, ValueEnvelope::data)
                                .withVar("cepState", CelVarType.DYN, v ->
                                        java.util.Objects.requireNonNull(v.cepState(), "ValueEnvelope.cepState() is null"))
                                // Coerce CEL result into a new ValueEnvelope
                                .withResultCoercer((ve, out) -> {
                                    if (out == null) {
                                        // Policy: keep original message if rule returns null
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

        // Preload in deterministic order (nice for tests/logs)
        var keys = legalKeys(keyToCel);
        for (String key : keys) {
            String source = java.util.Objects.requireNonNull(
                    keyToCel.get(key),
                    "No CEL source for key " + key + " Legal values: " + keys
            );
            ruleCache.populate(key, source);
        }

        return new CelInlineLogicExecutor<>(ruleCache, keyToCel);
    }

    private static java.util.List<String> legalKeys(java.util.Map<String, String> keyToCel) {
        var result = new java.util.ArrayList<String>(keyToCel.keySet());
        result.sort(String::compareTo);
        return result;
    }
}
