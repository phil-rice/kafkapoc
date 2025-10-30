// CelExecutionTest.java
package com.hcltech.rmg.celimpl;

import com.hcltech.rmg.celcore.CelVarType;
import com.hcltech.rmg.celcore.CompiledCelRule;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class CelExecutionTest {

    @Test
    @DisplayName("boolean rule with nested/bracket access; multiple inputs")
    void boolean_rule_with_nested_and_multiple_inputs() {
        String src = """
                user == "admin" &&
                ("amount" in data ? data["amount"] > 100 : false) &&
                data["order"]["total"] > 0
                """;

        CompiledCelRule<Map<String, ?>, Object> compiled = CelRuleBuilders.newRuleBuilder
                .<Map<String, ?>, Object>createCelRuleBuilder(src)
                .withVar("data", CelVarType.DYN, in -> in.get("data"))
                .withVar("user", CelVarType.STRING, in -> in.get("user"))
                .compile()
                .valueOrThrow();

        var ok = Map.of(
                "user", "admin",
                "data", Map.of("amount", 150L, "order", Map.of("total", 200L))
        );
        var badUser = Map.of(
                "user", "guest",
                "data", Map.of("amount", 150L, "order", Map.of("total", 200L))
        );

        assertEquals(Boolean.TRUE, compiled.executor().execute(ok));
        assertEquals(Boolean.FALSE, compiled.executor().execute(badUser));
    }

    @Test
    @DisplayName("presence checks guard missing keys/types")
    void presence_checks_guard_missing_keys() {
        String src = """
                ("amount" in data && type(data["amount"]) == int) ? data["amount"] > 100 : false
                """;

        var compiled = CelRuleBuilders.newRuleBuilder
                .<Map<String, ?>, Object>createCelRuleBuilder(src)
                .withVar("data", CelVarType.DYN, in -> in)
                .compile()
                .valueOrThrow();

        var missing = Map.of("order", Map.of("total", 20L)); // amount missing
        assertEquals(Boolean.FALSE, compiled.executor().execute(missing));

        var presentButSmall = Map.of("amount", 90L);
        assertEquals(Boolean.FALSE, compiled.executor().execute(presentButSmall));

        var presentAndLarge = Map.of("amount", 150L);
        assertEquals(Boolean.TRUE, compiled.executor().execute(presentAndLarge));
    }

    @Test
    @DisplayName("runtime type errors throw CelExecutionException")
    void runtime_type_errors_surface() {
        String src = "data[\"amount\"] + 1"; // amount is a string at runtime

        var compiled = CelRuleBuilders.newRuleBuilder
                .<Map<String, ?>, Object>createCelRuleBuilder(src)
                .withVar("data", CelVarType.DYN, in -> in)
                .compile()
                .valueOrThrow();

        var badInput = Map.of("amount", "not-a-number");
        assertThrows(CelExecutionException.class, () -> compiled.executor().execute(badInput));
    }

    @Test
    @DisplayName("numeric and string results")
    void numeric_and_string_results() {
        var sum = CelRuleBuilders.newRuleBuilder
                .<Map<String, ?>, Object>createCelRuleBuilder("data[\"a\"] + data[\"b\"]")
                .withVar("data", CelVarType.DYN, in -> in)
                .compile()
                .valueOrThrow();

        var greet = CelRuleBuilders.newRuleBuilder
                .<Map<String, ?>, Object>createCelRuleBuilder("'hello ' + user")
                .withVar("user", CelVarType.STRING, in -> in.get("user"))
                .compile()
                .valueOrThrow();

        assertEquals(5L, sum.executor().execute(Map.of("a", 2L, "b", 3L))); // CEL int -> Long
        assertEquals("hello world", greet.executor().execute(Map.of("user", "world")));
    }
}
