package com.hcltech.rmg.celcore.cache;

import com.hcltech.rmg.celcore.CompiledCelRule;
import com.hcltech.rmg.celcore.CompiledCelRuleWithDetails;
import com.hcltech.rmg.common.errorsor.ErrorsOr;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Contract tests for any PopulateCelRuleCache implementation.
 *
 * Assumptions:
 *  - Single-argument executors (no separate context arg)
 *  - Input = Map<String, ?>
 *  - Output = Integer (these tests)
 *  - populate() returns CompiledCelRuleWithDetails for optional metadata inspection
 */
public abstract class AbstractCelRuleCacheContractTest {

    /** Provide a compiler: source -> CompiledCelRuleWithDetails<Map<String, ?>, Integer>. */
    protected abstract Function<String, ErrorsOr<CompiledCelRuleWithDetails<Map<String, ?>, Integer>>> intCompiler();

    /** Provide a cache instance using the given compiler and overwrite flag. */
    protected abstract PopulateCelRuleCache<Map<String, ?>, Integer> newCache(
            Function<String, ErrorsOr<CompiledCelRuleWithDetails<Map<String, ?>, Integer>>> compileFn,
            boolean overwriteOnPopulate
    );

    @Test
    @DisplayName("populate then get: executes compiled rule (sum)")
    void populateThenGet_executes() {
        var cache = newCache(intCompiler(), true);

        var populated = cache.populate("sum", "data['a'] + data['b']").valueOrThrow();
        assertNotNull(populated.source(), "source() should be present");
        assertEquals(5, populated.executor().execute(Map.of("a", 2L, "b", 3L)));

        // get() returns a rule that executes identically
        CompiledCelRule<Map<String, ?>, Integer> fromCache = cache.get("sum");
        assertEquals(5, fromCache.executor().execute(Map.of("a", 2L, "b", 3L)));
    }

    @Test
    @DisplayName("get on missing key throws CelRuleNotFoundException")
    void getMissingThrows() {
        var cache = newCache(intCompiler(), true);
        assertThrows(CelRuleNotFoundException.class, () -> cache.get("nope"));
    }

    @Test
    @DisplayName("populate surfaces compile error via ErrorsOr")
    void populateCompileErrorSurfaced() {
        var cache = newCache(intCompiler(), true);

        var res = cache.populate("bad", "data['x'"); // parse error
        assertTrue(res.isError(), "expected compile error");
        String all = String.join(" | ", res.errorsOrThrow()).toLowerCase();
        assertTrue(all.contains("compile") || all.contains("validate"), all);
    }

    @Test
    @DisplayName("overwriteOnPopulate=true replaces existing entry")
    void overwriteReplaces() {
        var cache = newCache(intCompiler(), true);

        var first = cache.populate("k", "1 + 1").valueOrThrow();
        assertEquals(2, first.executor().execute(Map.of()));

        var second = cache.populate("k", "2 + 1").valueOrThrow();
        assertEquals(3, second.executor().execute(Map.of()));

        assertEquals(3, cache.get("k").executor().execute(Map.of()));
    }

    @Test
    @DisplayName("overwriteOnPopulate=false keeps first entry (no-op on duplicate)")
    void noOverwriteKeepsFirst() {
        var cache = newCache(intCompiler(), false);

        var first = cache.populate("k", "1 + 1").valueOrThrow();
        assertEquals(2, first.executor().execute(Map.of()));

        // Attempt to replace; should return existing compiled rule and keep old behavior
        var second = cache.populate("k", "2 + 1").valueOrThrow();
        assertEquals(2, second.executor().execute(Map.of()));

        assertEquals(2, cache.get("k").executor().execute(Map.of()));
    }
}
