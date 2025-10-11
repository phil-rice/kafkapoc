package com.hcltech.rmg.execution.bizlogic;

import com.hcltech.rmg.celcore.CelRuleBuilderFactory;
import com.hcltech.rmg.celcore.CelVarType;
import com.hcltech.rmg.celcore.cache.CelRuleCache;
import com.hcltech.rmg.celcore.cache.InMemoryCelRuleCache;
import com.hcltech.rmg.config.bizlogic.CelInlineLogic;
import com.hcltech.rmg.config.config.BehaviorConfig;
import com.hcltech.rmg.config.config.BehaviorConfigVisitor;
import com.hcltech.rmg.config.config.BehaviorConfigWalker;
import com.hcltech.rmg.execution.aspects.AspectExecutor;
import com.hcltech.rmg.messages.ValueEnvelope;

import java.util.*;

public class CelInlineLogicExecutor<CepState, Msg>
        implements AspectExecutor<CelInlineLogic, ValueEnvelope<CepState, Msg>, ValueEnvelope<CepState, Msg>> {

    private final CelRuleCache<ValueEnvelope<CepState, Msg>, ValueEnvelope<CepState, Msg>> ruleCache;

    public CelInlineLogicExecutor(CelRuleCache<ValueEnvelope<CepState, Msg>, ValueEnvelope<CepState, Msg>> ruleCache) {
        this.ruleCache = ruleCache;
    }

    @Override
    public ValueEnvelope<CepState, Msg> execute(String key,
                                                CelInlineLogic celInlineLogic,
                                                ValueEnvelope<CepState, Msg> input) {
        return ruleCache.get(key).executor().execute(input);
    }

    /** New: require the message class for safe, contextual coercion. */
    public static <CepState, Msg> CelInlineLogicExecutor<CepState, Msg> create(
            CelRuleBuilderFactory ruleBuilderFactory,
            BehaviorConfig config,
            Class<Msg> msgClass) {

        var keyToCel = new HashMap<String, String>();

        BehaviorConfigWalker.walk(config, new BehaviorConfigVisitor() {
            @Override
            public void onCelInlineLogic(String eventName, String moduleName, CelInlineLogic b) {
                String key = BehaviorConfig.configKey(moduleName, BehaviorConfig.bizlogicAspectName, eventName);
                keyToCel.put(key, b.cel());
            }
        });

        InMemoryCelRuleCache<ValueEnvelope<CepState, Msg>, ValueEnvelope<CepState, Msg>> ruleCache =
                new InMemoryCelRuleCache<>(
                        key -> {
                            String source = keyToCel.get(key);
                            if (source == null || source.isBlank()) {
                                throw new IllegalArgumentException(
                                        "No CEL source for key " + key + " Legal values: " + legalKeys(keyToCel));
                            }
                            return ruleBuilderFactory
                                    .<ValueEnvelope<CepState, Msg>, ValueEnvelope<CepState, Msg>>createCelRuleBuilder(source)
                                    .withVar("message", CelVarType.DYN, ValueEnvelope::data)
                                    // Upstream NPE check: header() must exist
                                    .withVar("cepState", CelVarType.DYN, v ->
                                            Objects.requireNonNull(v.header(), "ValueEnvelope.header() is null").cepState())
                                    .withResultCoercer((ve, out) -> {
                                        if (out == null) {
                                            // Policy: keep original data if rule yields null
                                            return ve;
                                        }
                                        if (!msgClass.isInstance(out)) {
                                            throw new IllegalArgumentException(
                                                    "CEL result type does not match expected message type for key "
                                                            + key + ". Expected: " + msgClass.getName()
                                                            + ", got: " + out.getClass().getName());
                                        }
                                        return ve.withData(msgClass.cast(out));
                                    })
                                    .compile();
                        },
                        /* overwriteOnPopulate = */ false
                );

        var keys = legalKeys(keyToCel);
        for (String key : keys) {
            ruleCache.populate(key, keyToCel.get(key));
        }
        return new CelInlineLogicExecutor<>(ruleCache);
    }

    private static List<String> legalKeys(Map<String, String> keyToCel) {
        ArrayList<String> result = new ArrayList<>(keyToCel.keySet());
        result.sort(String::compareTo);
        return result;
    }
}
