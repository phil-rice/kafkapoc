// AbstractCelValidationExecutorContractTest.java
package com.hcltech.rmg.execution.validation.cel;

import com.hcltech.rmg.celcore.CelRuleBuilder;
import com.hcltech.rmg.celcore.CelRuleBuilderFactory;
import com.hcltech.rmg.celcore.CelVarType;
import com.hcltech.rmg.celcore.CompiledCelRuleWithDetails;
import com.hcltech.rmg.celcore.cache.InMemoryCelRuleCache;
import com.hcltech.rmg.common.errorsor.ErrorsOr;
import com.hcltech.rmg.config.validation.CelValidation;
import com.hcltech.rmg.messages.EnvelopeHeader;
import com.hcltech.rmg.messages.ValueEnvelope;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Contract tests for CelValidationExecutor using REAL builder & cache.
 * Subclass this and implement {@link #realFactory()} to plug in your production factory.
 */
public abstract class AsbtractCelValidationExecutorTest {

    /**
     * Supply the production factory, e.g. CelRuleBuilders.newRuleBuilder.
     */
    protected abstract CelRuleBuilderFactory realFactory();

    /* --------------------- Counting wrapper (real builder, just counts) --------------------- */
    protected static final class CountingFactory implements CelRuleBuilderFactory {
        private final CelRuleBuilderFactory delegate;
        private final AtomicInteger compileCount = new AtomicInteger();

        public CountingFactory(CelRuleBuilderFactory delegate) {
            this.delegate = Objects.requireNonNull(delegate);
        }

        public int totalCompiles() {
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

    /* --------------------- Helpers --------------------- */

    private static List<String> sortedKeys(Map<String, String> m) {
        var list = new ArrayList<>(m.keySet());
        list.sort(String::compareTo);
        return list;
    }

    /**
     * Build executor like production create(), but from a simple key→cel map (no BehaviorConfig needed).
     */
    protected <S, M> CelValidationExecutor<S, M> makeExecutorLikeCreate(
            CelRuleBuilderFactory factory, Map<String, String> keyToCel) {

        var ruleCache = new InMemoryCelRuleCache<>(
                key -> {
                    String source = Objects.requireNonNull(
                            keyToCel.get(key),
                            "No CEL source for key " + key + " Legal values: " + sortedKeys(keyToCel)
                    );
                    return factory.<ValueEnvelope<S, M>, List<String>>createCelRuleBuilder(source)
                            .withVar("msg", CelVarType.DYN, ValueEnvelope::data)
                            .withVar("cepState", CelVarType.DYN, v -> v.header().cepState())
                            .compile();
                },
                /* overwriteOnPopulate */ false
        );

        for (String k : sortedKeys(keyToCel)) {
            ruleCache.populate(k, keyToCel.get(k));
        }
        return new CelValidationExecutor<>(ruleCache);
    }

    private static <S, M> ValueEnvelope<S, M> envelope(S cepState, M msg) {
        var header = new EnvelopeHeader<>(
                "domType", "domId", "evt",
                /* rawMessage */ null,
                /* parameters */ null,
                /* config */ null,
                cepState
        );
        return new ValueEnvelope<>(header, msg, List.of());
    }

    /* --------------------- Tests --------------------- */

    @Test
    @DisplayName("execute runs compiled rule and returns value")
    void execute_runs_compiled_rule_and_returns_value() {
        var factory = realFactory();
        String cel = "['executed:' + msg]"; // real CEL
        var exec = makeExecutorLikeCreate(factory, Map.of("mod:validation:eventA", cel));

        var out = exec.execute("mod:validation:eventA", new CelValidation("IGNORED"), envelope("S", "input"));
        assertEquals(List.of("executed:input"), out);
    }

    @Test
    @DisplayName("runtime CEL ignored; compile snapshot by key")
    void runtime_cel_is_ignored_compilation_is_by_key_snapshot() {
        var factory = realFactory();
        var exec = makeExecutorLikeCreate(factory, Map.of("k", "['SNAP:' + msg]"));

        var out = exec.execute("k", new CelValidation("DIFFERENT AT RUNTIME"), envelope("S", "x"));
        assertEquals(List.of("SNAP:x"), out);
    }

    @Test
    @DisplayName("caches one compile per key across multiple executes")
    void caches_one_compile_per_key_across_multiple_executes() {
        var counting = new CountingFactory(realFactory());
        String src = "['X:' + msg]";
        var exec = makeExecutorLikeCreate(counting, Map.of("k", src));

        var r1 = exec.execute("k", new CelValidation("ignored"), envelope("S", "a"));
        var r2 = exec.execute("k", new CelValidation("ignored"), envelope("S", "b"));

        assertEquals(List.of("X:a"), r1);
        assertEquals(List.of("X:b"), r2);
        assertEquals(1, counting.totalCompiles(), "one compile for the key");
    }

    @Test
    @DisplayName("preload compiles all keys once")
    void preload_compiles_all_keys_once() {
        var counting = new CountingFactory(realFactory());
        var map = new LinkedHashMap<String, String>();
        map.put("k2", "['2:' + msg]");
        map.put("k1", "['1:' + msg]");
        map.put("k3", "['3:' + msg]");

        makeExecutorLikeCreate(counting, map); // preload happens here

        assertEquals(3, counting.totalCompiles());
    }

    @Test
    @DisplayName("missing key throws with sorted legal keys in message")
    void missing_key_throws_npe_with_sorted_legal_keys_in_message() {
        var factory = realFactory();
        var keyToCel = new LinkedHashMap<String, String>();
        keyToCel.put("b/key", "['b:' + msg]");
        keyToCel.put("a/key", "['a:' + msg]");

        var exec = makeExecutorLikeCreate(factory, keyToCel);

        var ex = assertThrows(NullPointerException.class, () ->
                exec.execute("z/missing", new CelValidation("x"), envelope("S", "input"))
        );
        assertTrue(ex.getMessage().contains("No CEL source for key z/missing"));
        assertTrue(ex.getMessage().contains("Legal values: [a/key, b/key]"));
    }
}
