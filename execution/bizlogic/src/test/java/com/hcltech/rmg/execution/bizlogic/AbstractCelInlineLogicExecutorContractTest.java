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

public abstract class AbstractCelInlineLogicExecutorContractTest {


    protected abstract CelRuleBuilderFactory realFactory();

    static final class CountingFactory implements CelRuleBuilderFactory {
        private final CelRuleBuilderFactory delegate;
        private final AtomicInteger compileCount = new AtomicInteger();

        CountingFactory(CelRuleBuilderFactory delegate) { this.delegate = delegate; }

        int totalCompiles() { return compileCount.get(); }

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

    private static final String PARAM_KEY = "paramKey=testParams";

    private static BehaviorConfig behaviorOf(Map<String, Map<String, String>> eventToModuleToCel) {
        Map<String, AspectMap> events = new LinkedHashMap<>();
        eventToModuleToCel.forEach((event, modMap) -> {
            Map<String, com.hcltech.rmg.config.bizlogic.BizLogicAspect> biz = new LinkedHashMap<>();
            modMap.forEach((module, cel) -> biz.put(module, new CelInlineLogic(cel)));
            events.put(event, new AspectMap(Map.of(), Map.of(), Map.of(), biz));
        });
        return new BehaviorConfig(events);
    }

    private static Configs configsOf(String paramKey, Map<String, Map<String, String>> eventToModuleToCel) {
        BehaviorConfig behavior = behaviorOf(eventToModuleToCel);
        Config cfg = new Config(behavior, /* parameterConfig */ null, /* xmlSchemaPath */ null);
        return new Configs(Map.of(paramKey, cfg));
    }

    // Header no longer carries cepState — build a basic header here.
    private static <S> EnvelopeHeader<S> header() {
        return new EnvelopeHeader<>("domType", "evt", /* raw */ null, /* params */ null, /* config */ null, Map.of());
    }

    private static String keyFor(String eventName, String moduleName) {
        return Configs.composeKey(PARAM_KEY, eventName, moduleName);
    }

    @Test
    @DisplayName("real CEL: \"'NEW:' + message\" updates envelope data")
    void realCel_happyPath_updatesMessage() {
        CelRuleBuilderFactory factory = realFactory();

        String cel = "'NEW:' + message";
        Configs cfgs = configsOf(PARAM_KEY, Map.of("ev", Map.of("mod", cel)));

        CelInlineLogicExecutor<String, String> exec =
                CelInlineLogicExecutor.create(factory, cfgs, String.class);

        // CEP state now lives on the envelope (3rd arg)
        ValueEnvelope<String, String> in = new ValueEnvelope<>(header(), "old", "CEP", List.of());
        ValueEnvelope<String, String> out = exec.execute(keyFor("ev", "mod"), new CelInlineLogic("ignored"), in);

        assertEquals("NEW:old", out.data());
        assertEquals("CEP", out.cepState()); // was out.header().cepState()
        assertSame(in.header(), out.header(), "withData should preserve header instance");
    }

    @Test
    @DisplayName("typed coercion: CEL numeric result with msgClass=String throws")
    void typedCoercion_mismatch_throws() {
        CelRuleBuilderFactory factory = realFactory();

        String cel = "123";
        Configs cfgs = configsOf(PARAM_KEY, Map.of("ev", Map.of("mod", cel)));

        CelInlineLogicExecutor<String, String> exec =
                CelInlineLogicExecutor.create(factory, cfgs, String.class);

        ValueEnvelope<String, String> in = new ValueEnvelope<>(header(), "old", "S", List.of());

        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
                () -> exec.execute(keyFor("ev", "mod"), new CelInlineLogic("ignored"), in));
        assertTrue(ex.getMessage().toLowerCase().contains("expected: java.lang.string"));
    }

    @Test
    @DisplayName("snapshot: runtime CelInlineLogic is ignored; compilation is by key from Configs")
    void snapshot_runtimeCelIgnored_compilesByKey() {
        CelRuleBuilderFactory factory = realFactory();

        String preloaded = "'SNAP:' + message";
        Configs cfgs = configsOf(PARAM_KEY, Map.of("ev", Map.of("mod", preloaded)));

        CelInlineLogicExecutor<String, String> exec =
                CelInlineLogicExecutor.create(factory, cfgs, String.class);

        ValueEnvelope<String, String> in = new ValueEnvelope<>(header(), "old", "X", List.of());

        ValueEnvelope<String, String> out =
                exec.execute(keyFor("ev", "mod"), new CelInlineLogic("DIFFERENT AT RUNTIME"), in);

        assertEquals("SNAP:old", out.data());
    }

    @Test
    @DisplayName("preload compiles all keys once (deterministic)")
    void preload_compiles_all_keys_once() {
        CountingFactory counting = new CountingFactory(realFactory());

        Configs cfgs = configsOf(PARAM_KEY, Map.of(
                "ev2", Map.of("mod", "'2:' + message"),
                "ev1", Map.of("mod", "'1:' + message"),
                "ev3", Map.of("mod", "'3:' + message")
        ));

        CelInlineLogicExecutor<String, String> exec =
                CelInlineLogicExecutor.create(counting, cfgs, String.class);

        assertEquals(3, counting.totalCompiles(), "expected one compile per key at preload");

        ValueEnvelope<String, String> in = new ValueEnvelope<>(header(), "z", "S", List.of());
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

        ValueEnvelope<String, String> in = new ValueEnvelope<>(header(), "x", "S", List.of());

        String missing = keyFor("missingEv", "mod");
        assertThrows(RuntimeException.class, () ->
                exec.execute(missing, new CelInlineLogic("ignored"), in));
    }
}
