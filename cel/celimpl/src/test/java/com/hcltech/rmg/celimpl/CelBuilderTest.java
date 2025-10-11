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
                .withResultCoercer((in, o) -> { throw new IllegalArgumentException("Expected list"); })
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
}
