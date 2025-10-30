// CelBuilderTest.java
package com.hcltech.rmg.celimpl;

import com.hcltech.rmg.celcore.CelRuleBuilder;
import com.hcltech.rmg.celcore.CelVarType;
import com.hcltech.rmg.celcore.CompiledCelRule;
import com.hcltech.rmg.celcore.CompiledCelRuleWithDetails;
import com.hcltech.rmg.common.errorsor.ErrorsOr;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;

import static org.junit.jupiter.api.Assertions.*;

public class CelBuilderTest {

    @Test
    @DisplayName("compile() success with declared vars; executor runs and returns value")
    void compileAndExecute_ok() {
        String src = "domainId == 'A' ? ['routeA'] : (int(data.amount) > 100 ? ['discount'] : [])";

        CelRuleBuilder<Map<String, ?>, List<String>> b =
                CelRuleBuilders.newRuleBuilder
                        .<Map<String, ?>, List<String>>createCelRuleBuilder(src)
                        .withVar("data", CelVarType.DYN, in -> in) // expose whole input as 'data'
                        .withVar("domainId", CelVarType.STRING, in -> {
                            Object dom = in.get("domainId");
                            return dom != null ? dom : "B";
                        })
                        .withResultCoercer((in, o) -> (List<String>) o);

        CompiledCelRule<Map<String, ?>, List<String>> compiled = b.compile().valueOrThrow();

        // domainId != 'A' and amount <= 100 → []
        assertEquals(List.of(), compiled.executor().execute(Map.of("amount", 50L, "domainId", "B")));
        // amount > 100 → ['discount']
        assertEquals(List.of("discount"), compiled.executor().execute(Map.of("amount", 150L, "domainId", "B")));
        // domainId == 'A' → ['routeA']
        assertEquals(List.of("routeA"), compiled.executor().execute(Map.of("amount", 10L, "domainId", "A")));
    }

    @Test
    @DisplayName("withResultCoercer validates and fails when wrong shape is returned")
    void resultCoercer_validates() {
        String src = "1 + 1"; // returns a number

        var compiled = CelRuleBuilders.newRuleBuilder
                .<Map<String, ?>, Integer>createCelRuleBuilder(src)
                .withResultCoercer((in, o) -> {
                    if (o instanceof Number n) return n.intValue();
                    throw new IllegalArgumentException("Expected number");
                })
                .compile()
                .valueOrThrow();

        assertEquals(2, compiled.executor().execute(Map.of()));

        // strict coercer that always throws
        var compiledBad = CelRuleBuilders.newRuleBuilder
                .<Map<String, ?>, List<String>>createCelRuleBuilder(src)
                .withResultCoercer((in, o) -> {
                    throw new IllegalArgumentException("Expected list");
                })
                .compile()
                .valueOrThrow();

        assertThrows(IllegalArgumentException.class, () -> compiledBad.executor().execute(Map.of()));
    }

    @Test
    @DisplayName("compile() surfaces CEL compile-time error")
    void compileError_isSurfaced() {
        String bad = "data['x'"; // parse error
        ErrorsOr<CompiledCelRuleWithDetails<Map<String, ?>, Object>> compiled =
                CelRuleBuilders.newRuleBuilder
                        .<Map<String, ?>, Object>createCelRuleBuilder(bad)
                        .compile();

        assertTrue(compiled.isError(), "Expected compile error");
        String all = String.join(" | ", compiled.errorsOrThrow()).toLowerCase();
        assertTrue(all.contains("compile") || all.contains("validate"), all);
    }

    @Test
    @DisplayName("compile(): validation failure surfaces as ErrorsOr.errors with CEL compile/validate prefix")
    void compile_validation_failure_returns_error() {
        // syntactically invalid CEL (triggers dev.cel CelValidationException)
        String bad = "data["; // unmatched bracket
        ErrorsOr<CompiledCelRuleWithDetails<Map<String, ?>, Object>> eo =
                CelRuleBuilders.newRuleBuilder
                        .<Map<String, ?>, Object>createCelRuleBuilder(bad)
                        .withVar("data", CelVarType.DYN, in -> in)
                        .compile();

        assertTrue(eo.isError(), "Expected compile error for invalid CEL");
        String msg = String.join(" | ", eo.errorsOrThrow());
        assertTrue(msg.startsWith("CEL compile/validate error:"), "Message should start with CEL compile/validate error, got: " + msg);
    }

    @Test
    @DisplayName("withActivationFiller: overrides declared key and can add extras (ignored by CEL if undeclared)")
    void withActivationFiller_overrides_and_adds_extras() {
        String src = "x == 'OVERRIDE'"; // declared var x, filler will override it
        AtomicInteger fillerCalls = new AtomicInteger();

        var compiled = CelRuleBuilders.newRuleBuilder
                .<Map<String, ?>, Object>createCelRuleBuilder(src)
                .withVar("x", CelVarType.STRING, in -> "BASE")   // declared getter
                .withActivationFiller(override("x", "OVERRIDE", fillerCalls)) // override declared + add extra
                .compile()
                .valueOrThrow();

        Object out = compiled.executor().execute(Map.of());
        assertEquals(Boolean.TRUE, out, "Activation filler should override declared 'x'");

        // Execute again to be sure we don't rely on per-call allocation
        assertEquals(Boolean.TRUE, compiled.executor().execute(Map.of()));
        assertTrue(fillerCalls.get() >= 2, "Filler should run every execute()");
    }

    @Test
    @DisplayName("withResultCoercer: raw result can be transformed with input-aware coercer")
    void withResultCoercer_transforms_raw_result() {
        String src = "data['a'] + data['b']"; // 64-bit CEL int
        var compiled = CelRuleBuilders.newRuleBuilder
                .<Map<String, ?>, String>createCelRuleBuilder(src)
                .withVar("data", CelVarType.DYN, in -> in)
                .withResultCoercer((in, raw) -> "sum=" + raw)
                .compile()
                .valueOrThrow();

        String out = compiled.executor().execute(Map.of("a", 2L, "b", 3L));
        assertEquals("sum=5", out);
    }

    @Test
    @DisplayName("no declared vars: filler NOOP branch and stableMapCapacity(n<=0) are exercised")
    void zero_vars_happy_path() {
        var compiled = CelRuleBuilders.newRuleBuilder
                .<Object, Object>createCelRuleBuilder("'hi'")
                .compile()
                .valueOrThrow();

        assertEquals("hi", compiled.executor().execute(new Object()));
        // Also check metadata contract
        assertEquals("'hi'", ((CompiledCelRuleWithDetails<?, ?>) compiled).source()); // source() exposed
        assertTrue(((CompiledCelRuleWithDetails<?, ?>) compiled).pathsFor("anything").isEmpty(),
                "pathsFor(var) is empty placeholder today");
    }

    // --- helper to build a filler that overrides one key and adds an extra ---
    private static <I> BiConsumer<I, Map<String, Object>> override(String key, Object value, AtomicInteger calls) {
        return (in, act) -> {
            calls.incrementAndGet();
            act.put(key, value);        // override declared key
            act.put("extra_key", 123);  // add extra undeclared key (CEL ignores undeclared)
        };
    }
}
