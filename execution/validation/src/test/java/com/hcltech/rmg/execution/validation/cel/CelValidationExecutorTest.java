package com.hcltech.rmg.execution.validation.cel;

import com.hcltech.rmg.celcore.*;
import com.hcltech.rmg.celcore.cache.InMemoryRuleCache;
import com.hcltech.rmg.celcore.cache.RuleCache;
import com.hcltech.rmg.common.errorsor.ErrorsOr;
import com.hcltech.rmg.config.validation.CelValidation;
import org.junit.jupiter.api.Test;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.*;

class CelValidationExecutorTest {

    // ---------- Helpers ----------

    /** Minimal compiled rule returning a single string derived from the source. */
    private static CompiledRule<String, List<String>> makeCompiled(String src) {
        return new CompiledRule<>() {
            @Override public String source() { return src; }
            @Override public RuleUsage usage() {
                return new RuleUsage() {
                    @Override public List<String> inputPaths() { return List.of(); }
                    @Override public List<String> contextPaths() { return List.of(); }
                };
            }
            @Override public RuleExecutor<String, List<String>> executor() {
                // Return a recognizable payload to assert execution happened
                return (inp, ctx) -> ErrorsOr.lift(List.of("executed:" + src));
            }
        };
    }

    /** Fake factory that can also simulate compile failures for specific sources. */
    static final class FakeRuleBuilderFactory implements RuleBuilderFactory {
        private final Set<String> errorSources;
        private final AtomicInteger builds = new AtomicInteger();
        private final Map<String, Integer> buildsBySource = new HashMap<>();

        FakeRuleBuilderFactory(String... errorSources) {
            this.errorSources = Set.of(errorSources);
        }

        int totalBuilds() { return builds.get(); }
        int buildsFor(String source) { return buildsBySource.getOrDefault(source, 0); }

        @Override
        public <I, O> RuleBuilder<I, O> newRuleBuilder(String source) {
            builds.incrementAndGet();
            buildsBySource.merge(source, 1, Integer::sum);
            return new RuleBuilder<>() {
                @Override
                public RuleBuilder<I, O> validateInput(Function<List<String>, List<String>> validator) {
                    throw new RuntimeException("not implemented");
                }

                @Override
                public RuleBuilder<I, O> validateContext(Function<List<String>, List<String>> validator) {
                    throw new RuntimeException("not implemented");

                }

                @Override
                public RuleBuilder<I, O> withVarNames(String inputVar, String contextVar) {
                    throw new RuntimeException("not implemented");
                }

                @Override
                public RuleBuilder<I, O> withResultCoercer(Function<Object, Object> coercer) {
                    throw new RuntimeException("not implemented");
                }

                @Override public ErrorsOr<CompiledRule<I, O>> compile() {
                    if (errorSources.contains(source)) {
                        return ErrorsOr.error("compile failed for: " + source);
                    }
                    //noinspection unchecked
                    return ErrorsOr.lift((CompiledRule<I, O>) makeCompiled(source));
                }
            };
        }
    }

    /** Sorted copy of keys, to match production error text. */
    private static List<String> sortedKeys(Map<String, String> m) {
        var list = new ArrayList<>(m.keySet());
        list.sort(String::compareTo);
        return list;
    }

    /** Build an executor with the exact same wiring as create(), but from a map. */
    private static <I> CelValidationExecutor<I> makeExecutorLikeCreate(RuleBuilderFactory f, Map<String,String> keyToCel) {
        RuleCache<I, List<String>> ruleCache =
                new InMemoryRuleCache<I, List<String>>(
                        key -> {
                            String source = java.util.Objects.requireNonNull(
                                    keyToCel.get(key),
                                    "No CEL source for key " + key + " Legal values: " + sortedKeys(keyToCel)
                            );
                            return f.<I, List<String>>newRuleBuilder(source).compile();
                        }
                ).preloadWith(sortedKeys(keyToCel));
        return new CelValidationExecutor<>(ruleCache, Map.of());
    }

    // ---------- Tests ----------

    @Test
    void execute_runs_compiled_rule_and_returns_value() {
        var factory = new FakeRuleBuilderFactory();
        var keyToCel = Map.of("mod/validation/eventA", "cel: A");
        var exec = makeExecutorLikeCreate(factory, keyToCel);

        var out = exec.execute(
                "mod/validation/eventA",
                List.of("mod"),
                "validation",
                new CelValidation("IGNORED"),
                "input"
        );

        assertTrue(out.isValue());
        assertEquals(List.of("executed:cel: A"), out.getValue().orElseThrow());
        assertEquals(1, factory.totalBuilds());
        assertEquals(1, factory.buildsFor("cel: A"));
    }

    @Test
    void runtime_cel_is_ignored_compilation_is_by_key_snapshot() {
        var factory = new FakeRuleBuilderFactory();
        var keyToCel = Map.of("k", "cel: SNAPSHOT");
        var exec = makeExecutorLikeCreate(factory, keyToCel);

        var out = exec.execute("k", List.of(), "validation", new CelValidation("DIFFERENT"), "input");

        assertTrue(out.isValue());
        assertEquals(List.of("executed:cel: SNAPSHOT"), out.getValue().orElseThrow());
        assertEquals(1, factory.totalBuilds());
        assertEquals(1, factory.buildsFor("cel: SNAPSHOT"));
    }

    @Test
    void caches_one_compile_per_key_across_multiple_executes() {
        var factory = new FakeRuleBuilderFactory();
        var keyToCel = Map.of("k", "cel: X");
        var exec = makeExecutorLikeCreate(factory, keyToCel);

        var cv = new CelValidation("whatever");
        var r1 = exec.execute("k", List.of(), "validation", cv, "in1");
        var r2 = exec.execute("k", List.of(), "validation", cv, "in2");

        assertTrue(r1.isValue());
        assertTrue(r2.isValue());
        assertEquals(1, factory.totalBuilds(), "one compile per key");
        assertEquals(1, factory.buildsFor("cel: X"));
    }

    @Test
    void preload_compiles_all_keys_once() {
        var factory = new FakeRuleBuilderFactory();
        var keyToCel = new LinkedHashMap<String,String>();
        keyToCel.put("k2", "cel: 2");
        keyToCel.put("k1", "cel: 1");
        keyToCel.put("k3", "cel: 3"); // deliberately unsorted to ensure sorting is used

        makeExecutorLikeCreate(factory, keyToCel); // constructor preloads

        assertEquals(3, factory.totalBuilds());
        assertEquals(1, factory.buildsFor("cel: 1"));
        assertEquals(1, factory.buildsFor("cel: 2"));
        assertEquals(1, factory.buildsFor("cel: 3"));
    }

    @Test
    void missing_key_throws_npe_with_sorted_legal_keys_in_message() {
        var factory = new FakeRuleBuilderFactory();
        var keyToCel = new LinkedHashMap<String,String>();
        keyToCel.put("b/key", "cel: b");
        keyToCel.put("a/key", "cel: a");
        var exec = makeExecutorLikeCreate(factory, keyToCel);

        var ex = assertThrows(NullPointerException.class, () ->
                exec.execute("z/missing", List.of(), "validation", new CelValidation("x"), "input")
        );

        // Message must include sorted legal keys from production helper
        assertTrue(ex.getMessage().contains("No CEL source for key z/missing"));
        assertTrue(ex.getMessage().contains("Legal values: [a/key, b/key]"));
    }

    @Test
    void compile_failure_is_cached_and_returned() {
        var factory = new FakeRuleBuilderFactory("cel: bad");
        var keyToCel = Map.of("badKey", "cel: bad");
        var exec = makeExecutorLikeCreate(factory, keyToCel);

        var r1 = exec.execute("badKey", List.of(), "validation", new CelValidation("x"), "in1");
        var r2 = exec.execute("badKey", List.of(), "validation", new CelValidation("y"), "in2");

        assertTrue(r1.isError());
        assertEquals(List.of("compile failed for: cel: bad"), r1.getErrors());
        assertTrue(r2.isError());
        assertEquals(1, factory.totalBuilds(), "failure compiled once and cached");
    }
}
