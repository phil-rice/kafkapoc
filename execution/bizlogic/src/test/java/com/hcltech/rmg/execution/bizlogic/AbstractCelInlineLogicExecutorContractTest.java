// AbstractCelInlineLogicExecutorContractTest.java
package com.hcltech.rmg.execution.bizlogic;

import com.hcltech.rmg.celcore.CelRuleBuilder;
import com.hcltech.rmg.celcore.CelRuleBuilderFactory;
import com.hcltech.rmg.celcore.CelVarType;
import com.hcltech.rmg.celcore.CompiledCelRuleWithDetails;
import com.hcltech.rmg.common.errorsor.ErrorsOr;
import com.hcltech.rmg.config.aspect.AspectMap;
import com.hcltech.rmg.config.bizlogic.CelInlineLogic;
import com.hcltech.rmg.config.config.BehaviorConfig;
import com.hcltech.rmg.config.config.Config;
import com.hcltech.rmg.config.configs.Configs;
import com.hcltech.rmg.messages.EnvelopeHeader;
import com.hcltech.rmg.messages.ValueEnvelope;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Contract tests for CelInlineLogicExecutor using REAL Configs and REAL create(...).
 * <p>
 * Subclass MUST implement realFactory() to return the production CelRuleBuilderFactory.
 */
public abstract class AbstractCelInlineLogicExecutorContractTest {

    /**
     * Provide your production CEL rule builder factory.
     */
    protected abstract CelRuleBuilderFactory realFactory();

    /* -------------------------- Counting wrapper -------------------------- */

    static final class CountingFactory implements CelRuleBuilderFactory {
        private final CelRuleBuilderFactory delegate;
        private final AtomicInteger compileCount = new AtomicInteger();

        CountingFactory(CelRuleBuilderFactory delegate) {
            this.delegate = delegate;
        }

        int totalCompiles() {
            return compileCount.get();
        }

        @Override
        public <I, O> CelRuleBuilder<I, O> createCelRuleBuilder(String source) {
            CelRuleBuilder<I, O> inner = delegate.createCelRuleBuilder(source);
            return new CelRuleBuilder<>() {
                @Override
                public CelRuleBuilder<I, O> withVar(String name, CelVarType type, java.util.function.Function<I, Object> getter) {
                    inner.withVar(name, type, getter);
                    return this;
                }

                @Override
                public CelRuleBuilder<I, O> withResultCoercer(java.util.function.BiFunction<I, Object, O> coercer) {
                    inner.withResultCoercer(coercer);
                    return this;
                }

                @Override
                public CelRuleBuilder<I, O> withActivationFiller(java.util.function.BiConsumer<I, Map<String, Object>> filler) {
                    inner.withActivationFiller(filler);
                    return this;
                }

                @Override
                public ErrorsOr<CompiledCelRuleWithDetails<I, O>> compile() {
                    compileCount.incrementAndGet();
                    return inner.compile();
                }
            };
        }
    }

    /* -------------------------- Config builders -------------------------- */

    private static final String PARAM_KEY = "paramKey=testParams";

    /**
     * Build BehaviorConfig from event → (module → cel) for inline bizlogic only.
     */
    private static BehaviorConfig behaviorOf(Map<String, Map<String, String>> eventToModuleToCel) {
        Map<String, AspectMap> events = new LinkedHashMap<>();
        eventToModuleToCel.forEach((event, modMap) -> {
            Map<String, com.hcltech.rmg.config.bizlogic.BizLogicAspect> biz = new LinkedHashMap<>();
            modMap.forEach((module, cel) -> biz.put(module, new CelInlineLogic(cel)));
            events.put(event, new AspectMap(Map.of(), Map.of(), Map.of(), biz));
        });
        return new BehaviorConfig(events);
    }

    /**
     * Wrap the BehaviorConfig in a Configs with a single paramKey entry.
     */
    private static Configs configsOf(String paramKey, Map<String, Map<String, String>> eventToModuleToCel) {
        BehaviorConfig behavior = behaviorOf(eventToModuleToCel);
        Config cfg = new Config(behavior, /* parameterConfig */ null, /* xmlSchemaPath */ null);
        return new Configs(Map.of(paramKey, cfg));
    }

    private static <S> EnvelopeHeader<S> header(S cepState) {
        return new EnvelopeHeader<>("domType", "domId", "evt", null, null, null, cepState);
    }

    /**
     * Helper to avoid swapping event/module by mistake.
     */
    private static String keyFor(String eventName, String moduleName) {
        return Configs.composeKey(PARAM_KEY, eventName, moduleName);
    }

    /* -------------------------- Tests -------------------------- */

    @Test
    @DisplayName("real CEL: \"'NEW:' + message\" updates envelope data")
    void realCel_happyPath_updatesMessage() {
        CelRuleBuilderFactory factory = realFactory();

        String cel = "'NEW:' + message";
        Configs cfgs = configsOf(PARAM_KEY, Map.of("ev", Map.of("mod", cel)));

        CelInlineLogicExecutor<String, String> exec =
                CelInlineLogicExecutor.create(factory, cfgs, String.class);

        ValueEnvelope<String, String> in = new ValueEnvelope<>(header("CEP"), "old", List.of());
        ValueEnvelope<String, String> out = exec.execute(keyFor("ev", "mod"), new CelInlineLogic("ignored"), in);

        assertEquals("NEW:old", out.data());
        assertEquals("CEP", out.header().cepState());
        assertSame(in.header(), out.header(), "withData should preserve header instance");
    }

    @Test
    @DisplayName("typed coercion: CEL numeric result with msgClass=String throws")
    void typedCoercion_mismatch_throws() {
        CelRuleBuilderFactory factory = realFactory();

        // CEL returns a number; coercer expects String.class -> IllegalArgumentException
        String cel = "123";
        Configs cfgs = configsOf(PARAM_KEY, Map.of("ev", Map.of("mod", cel)));

        CelInlineLogicExecutor<String, String> exec =
                CelInlineLogicExecutor.create(factory, cfgs, String.class);

        ValueEnvelope<String, String> in = new ValueEnvelope<>(header("S"), "old", List.of());

        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
                () -> exec.execute(keyFor("ev", "mod"), new CelInlineLogic("ignored"), in));
        assertTrue(ex.getMessage().toLowerCase().contains("expected: java.lang.string"));
    }

    @Test
    @DisplayName("upstream NPE: header is null → getter throws before CEL")
    void headerNull_triggersNpe() {
        CelRuleBuilderFactory factory = realFactory();

        String cel = "'anything'";
        Configs cfgs = configsOf(PARAM_KEY, Map.of("ev", Map.of("mod", cel)));

        CelInlineLogicExecutor<String, String> exec =
                CelInlineLogicExecutor.create(factory, cfgs, String.class);

        ValueEnvelope<String, String> in = new ValueEnvelope<>(/* header */ null, "old", List.of());

        NullPointerException npe = assertThrows(NullPointerException.class,
                () -> exec.execute(keyFor("ev", "mod"), new CelInlineLogic("ignored"), in));
        assertTrue(npe.getMessage().contains("ValueEnvelope.header() is null"));
    }

    @Test
    @DisplayName("snapshot: runtime CelInlineLogic is ignored; compilation is by key from Configs")
    void snapshot_runtimeCelIgnored_compilesByKey() {
        CelRuleBuilderFactory factory = realFactory();

        String preloaded = "'SNAP:' + message";
        Configs cfgs = configsOf(PARAM_KEY, Map.of("ev", Map.of("mod", preloaded)));

        CelInlineLogicExecutor<String, String> exec =
                CelInlineLogicExecutor.create(factory, cfgs, String.class);

        ValueEnvelope<String, String> in = new ValueEnvelope<>(header("X"), "old", List.of());

        ValueEnvelope<String, String> out =
                exec.execute(keyFor("ev", "mod"), new CelInlineLogic("DIFFERENT AT RUNTIME"), in);

        assertEquals("SNAP:old", out.data());
    }

    @Test
    @DisplayName("preload compiles all keys once (deterministic)")
    void preload_compiles_all_keys_once() {
        CountingFactory counting = new CountingFactory(realFactory());

        // 3 keys in config under the same param key
        Configs cfgs = configsOf(PARAM_KEY, Map.of(
                "ev2", Map.of("mod", "'2:' + message"),
                "ev1", Map.of("mod", "'1:' + message"),
                "ev3", Map.of("mod", "'3:' + message")
        ));

        CelInlineLogicExecutor<String, String> exec =
                CelInlineLogicExecutor.create(counting, cfgs, String.class);

        assertEquals(3, counting.totalCompiles(), "expected one compile per key at preload");

        // sanity: executing any key works
        ValueEnvelope<String, String> in = new ValueEnvelope<>(header("S"), "z", List.of());
        ValueEnvelope<String, String> out = exec.execute(keyFor("ev1", "mod"), new CelInlineLogic("ignored"), in);
        assertEquals("1:z", out.data());
    }

    @Test
    @DisplayName("executing a missing key throws")
    void missing_key_throws() {
        CelRuleBuilderFactory factory = realFactory();

        Configs cfgs = configsOf(PARAM_KEY, Map.of("ev", Map.of("mod", "'a:' + message")));

        CelInlineLogicExecutor<String, String> exec =
                CelInlineLogicExecutor.create(factory, cfgs, String.class);

        ValueEnvelope<String, String> in = new ValueEnvelope<>(header("S"), "x", List.of());

        String missing = keyFor("missingEv", "mod");
        assertThrows(RuntimeException.class, () ->
                exec.execute(missing, new CelInlineLogic("ignored"), in));
    }
}
