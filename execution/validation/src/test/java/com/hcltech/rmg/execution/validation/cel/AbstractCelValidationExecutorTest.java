package com.hcltech.rmg.execution.validation.cel;

import com.hcltech.rmg.celcore.CelRuleBuilder;
import com.hcltech.rmg.celcore.CelRuleBuilderFactory;
import com.hcltech.rmg.celcore.CelVarType;
import com.hcltech.rmg.celcore.CompiledCelRuleWithDetails;
import com.hcltech.rmg.celcore.cache.CelRuleNotFoundException;
import com.hcltech.rmg.common.errorsor.ErrorsOr;
import com.hcltech.rmg.config.aspect.AspectMap;
import com.hcltech.rmg.config.config.BehaviorConfig;
import com.hcltech.rmg.config.validation.CelValidation;
import com.hcltech.rmg.messages.EnvelopeHeader;
import com.hcltech.rmg.messages.ValueEnvelope;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Hexagonal, abstract contract tests for {@link CelValidationExecutor}.
 * Subclasses must supply the production {@link CelRuleBuilderFactory} via {@link #realFactory()}.
 *
 * These tests ONLY call {@link CelValidationExecutor#create(CelRuleBuilderFactory, BehaviorConfig)}
 * and execute rules via {@link CelValidationExecutor#execute(String, CelValidation, ValueEnvelope)}.
 */
public abstract class AbstractCelValidationExecutorTest {

    /** Provide your production factory here (e.g., CelRuleBuilders.newRuleBuilderFactory()). */
    protected abstract CelRuleBuilderFactory realFactory();

    /**
     * Wrapper around the real factory that counts how many times {@code compile()} is called.
     * Used to assert one-time pre-compilation and caching semantics — no test doubles of rules themselves.
     */
    protected static final class CountingFactory implements CelRuleBuilderFactory {
        private final CelRuleBuilderFactory delegate;
        private final AtomicInteger compiles = new AtomicInteger();

        public CountingFactory(CelRuleBuilderFactory delegate) {
            this.delegate = Objects.requireNonNull(delegate);
        }

        public int totalCompiles() { return compiles.get(); }

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
                    compiles.incrementAndGet();
                    return inner.compile();
                }
            };
        }
    }

    /** Build a real ValueEnvelope with a real EnvelopeHeader (unused fields kept null deliberately). */
    protected static <S, M> ValueEnvelope<S, M> env(S cepState, M msg) {
        var header = new EnvelopeHeader<>(
                "domType",
                "domId",
                "evt",          // eventType (executor exposes only msg + cepState vars)
                null,           // rawMessage
                null,           // parameters
                null,           // config
                cepState        // cepState variable bound in executor
        );
        return new ValueEnvelope<>(header, msg, List.of());
    }

    // ---------------------------------------------------------------------
    // create(...) CONTRACT TESTS (only real executor via static factory)
    // ---------------------------------------------------------------------

    @Test
    @DisplayName("create(...): preloads one compile per discovered key; execute runs compiled rules")
    void create_preloads_and_executes_from_behavior_config() {
        var counting = new CountingFactory(realFactory());

        // BehaviorConfig with two events and one validation module each.
        var orderEvt = new AspectMap(
                Map.of("orderMod", new CelValidation("['ORD:' + msg]")),
                Map.of(), Map.of(), Map.of()
        );
        var invoiceEvt = new AspectMap(
                Map.of("invoiceMod", new CelValidation("['INV:' + msg]")),
                Map.of(), Map.of(), Map.of()
        );
        var cfg = new BehaviorConfig(new LinkedHashMap<>(Map.of(
                "orderCreated", orderEvt,
                "invoicePosted", invoiceEvt
        )));

        CelValidationExecutor<String, String> exec = CelValidationExecutor.create(counting, cfg);

        // Preload once per key
        assertEquals(2, counting.totalCompiles(), "Expected one compile per derived key");

        String k1 = BehaviorConfig.configKey("orderMod", BehaviorConfig.validationAspectName, "orderCreated");
        String k2 = BehaviorConfig.configKey("invoiceMod", BehaviorConfig.validationAspectName, "invoicePosted");

        var r1 = exec.execute(k1, new CelValidation("IGNORED_AT_RUNTIME"), env("CEP", "abc"));
        var r2 = exec.execute(k2, new CelValidation("IGNORED_AT_RUNTIME"), env("CEP", "xyz"));

        assertEquals(List.of("ORD:abc"), r1);
        assertEquals(List.of("INV:xyz"), r2);
    }

    @Test
    @DisplayName("create(...): key format = module:validation:event")
    void create_uses_expected_key_format() {
        var counting = new CountingFactory(realFactory());

        var evt = new AspectMap(
                Map.of("modA", new CelValidation("['A:' + msg]")),
                Map.of(), Map.of(), Map.of()
        );
        var cfg = new BehaviorConfig(Map.of("eventX", evt));

        CelValidationExecutor<String, String> exec = CelValidationExecutor.create(counting, cfg);

        String expectedKey = BehaviorConfig.configKey("modA", BehaviorConfig.validationAspectName, "eventX");
        var out = exec.execute(expectedKey, new CelValidation("ignored"), env("S", "in"));
        assertEquals(List.of("A:in"), out);
    }

    @Test
    @DisplayName("create(...): repeated execute on same key does not recompile")
    void create_caches_one_compile_per_key_across_multiple_executes() {
        var counting = new CountingFactory(realFactory());

        var evt = new AspectMap(
                Map.of("kmod", new CelValidation("['X:' + msg]")),
                Map.of(), Map.of(), Map.of()
        );
        var cfg = new BehaviorConfig(Map.of("evt", evt));

        CelValidationExecutor<String, String> exec = CelValidationExecutor.create(counting, cfg);

        // Preload should have compiled exactly 1 rule
        assertEquals(1, counting.totalCompiles(), "One compile expected during preload");

        String k = BehaviorConfig.configKey("kmod", BehaviorConfig.validationAspectName, "evt");
        var r1 = exec.execute(k, new CelValidation("ignored"), env("S", "a"));
        var r2 = exec.execute(k, new CelValidation("ignored"), env("S", "b"));

        assertEquals(List.of("X:a"), r1);
        assertEquals(List.of("X:b"), r2);

        // Still exactly one compile (compile-once, execute-many)
        assertEquals(1, counting.totalCompiles(), "No recompile on repeated execute");
    }

    @Test
    @DisplayName("create(...): no validation rules → no compiles; executing unknown key throws")
    void create_with_no_rules_compiles_nothing_and_throws_on_unknown_key() {
        var counting = new CountingFactory(realFactory());

        var noVal = new AspectMap(Map.of(), Map.of(), Map.of(), Map.of());
        var cfg = new BehaviorConfig(Map.of("evt", noVal));

        CelValidationExecutor<String, String> exec = CelValidationExecutor.create(counting, cfg);
        assertEquals(0, counting.totalCompiles(), "No validation rules → no compiles");

        var ex = assertThrows(
                CelRuleNotFoundException.class,
                () -> exec.execute("unknown:key", new CelValidation("x"), env("S", "msg"))
        );
        assertTrue(ex.getMessage().startsWith("No compiled rule for key:"),
                "Should indicate missing compiled key, got: " + ex.getMessage());
    }
}
