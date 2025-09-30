package com.hcltech.rmg.celimpl;

import com.hcltech.rmg.common.errorsor.ErrorsOr;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class CelExecutionTest {

    @Test
    void execute_boolean_rule_with_bracket_access() {
        var src = """
      context["user"] == "admin" &&
      ("amount" in input ? input["amount"] > 100 : false) &&
      input["order"]["total"] > 0
      """;

        var compiledOr = CelBuilder.celBuilder(src).compile();
        assertTrue(compiledOr.isValue(), () -> "Compile errors: " + compiledOr.getErrors());

        var compiled = compiledOr.getValue().get();

        var input = Map.of(
                "amount", 150L,                    // Long, not Integer
                "order", Map.of("total", 200L)
        );
        var context = Map.of("user", "admin");

        ErrorsOr<Object> res = compiled.executor().execute(input, context);
        assertTrue(res.isValue(), () -> "Eval errors: " + res.getErrors());
        assertEquals(Boolean.TRUE, res.getValue().get());
    }

    @Test
    void execute_handles_missing_keys_safely_with_presence_checks() {
        var src = """
      // guard with presence checks
      ("amount" in input && type(input["amount"]) == int) ? input["amount"] > 100 : false
      """;

        var compiledOr = CelBuilder.celBuilder(src).compile();
        assertTrue(compiledOr.isValue(), () -> "Compile errors: " + compiledOr.getErrors());

        var compiled = compiledOr.getValue().get();

        var input = Map.of("order", Map.of("total", 20L)); // amount is missing
        Map<String,Object> context = Map.of();

        var res = compiled.executor().execute(input, context);
        assertTrue(res.isValue(), () -> "Eval errors: " + res.getErrors());
        assertEquals(Boolean.FALSE, res.getValue().get());
    }

    @Test
    void execute_reports_runtime_errors_in_errorsor() {
        // Intentionally wrong: do math on a string without guard
        var src = "input[\"amount\"] + 1";

        var compiledOr = CelBuilder.celBuilder(src).compile();
        assertTrue(compiledOr.isValue(), () -> "Compile errors: " + compiledOr.getErrors());

        var compiled = compiledOr.getValue().get();

        var input = Map.of("amount", "not-a-number");
        Map<String,Object> context = Map.of();

        var res = compiled.executor().execute(input, context);
        assertTrue(res.isError(), "Should report runtime evaluation error");
        assertTrue(res.getErrors().get(0).toLowerCase().contains("error"));
    }

    @Test
    void execute_with_custom_var_names() {
        var src = "ctx[\"role\"] == \"ops\" && inp[\"temp\"] > 30";

        var compiledOr = CelBuilder
                .celBuilder(src)
                .withVarNames("inp", "ctx")
                .compile();

        assertTrue(compiledOr.isValue(), () -> "Compile errors: " + compiledOr.getErrors());
        var compiled = compiledOr.getValue().get();

        var res = compiled.executor().execute(
                Map.of("temp", 35L),
                Map.of("role", "ops")
        );

        assertTrue(res.isValue(), () -> "Eval errors: " + res.getErrors());
        assertEquals(Boolean.TRUE, res.getValue().get());
    }

    @Test
    void execute_numeric_and_string_results() {
        var sum = CelBuilder.celBuilder("input[\"a\"] + input[\"b\"]").compile().valueOrThrow();
        var greet = CelBuilder.celBuilder("'hello ' + context[\"name\"]").compile().valueOrThrow();

        var sumRes = sum.executor().execute(Map.of("a", 2L, "b", 3L), Map.of());
        assertTrue(sumRes.isValue(), () -> "Eval errors: " + sumRes.getErrors());
        // CEL ints are 64-bit; expect Long
        assertEquals(5L, sumRes.getValue().get());

        var greetRes = greet.executor().execute(Map.of(), Map.of("name", "world"));
        assertTrue(greetRes.isValue(), () -> "Eval errors: " + greetRes.getErrors());
        assertEquals("hello world", greetRes.getValue().get());
    }
}
