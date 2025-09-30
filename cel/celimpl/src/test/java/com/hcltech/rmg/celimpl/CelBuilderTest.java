package com.hcltech.rmg.celimpl;

import com.hcltech.rmg.common.errorsor.ErrorsOr;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

class CelBuilderTest {

    @Test
    void compile_ok_without_validators() {
        var src = """
      context["user"] == "admin" &&
      ("amount" in input ? input["amount"] > 100 : false)
      """;
        var cel = CelBuilder.celBuilder(src).compile();

        assertTrue(cel.isValue(), () -> "Compile errors: " + cel.getErrors());
        var compiled = cel.getValue().get();
        assertNotNull(compiled.executor());
        assertNotNull(compiled.usage());
        assertTrue(compiled.usage().inputPaths().contains("amount"));
        assertTrue(compiled.usage().contextPaths().contains("user"));
    }

    @Test
    void compile_runs_input_and_context_validators() {
        var src = "context[\"user\"] == \"admin\" && input[\"order\"][\"total\"] > 0";

        var compiledOr = CelBuilder.celBuilder(src)
                .validateInput(paths -> {
                    var allowed = Set.of("order.total");
                    var errs = new ArrayList<String>();
                    for (var p : paths) if (!allowed.contains(p)) errs.add("Unknown input path: " + p);
                    return errs;
                })
                .validateContext(paths -> {
                    var required = Set.of("user");
                    var errs = new ArrayList<String>();
                    for (var r : required) if (!paths.contains(r)) errs.add("Missing context path: " + r);
                    return errs;
                })
                .compile();

        assertTrue(compiledOr.isValue(), () -> "Validator errors: " + compiledOr.getErrors());
    }

    @Test
    void compile_collects_validator_errors_distinctly() {
        var src = "context[\"user\"] == \"admin\" && input[\"order\"][\"total\"] > 0";

        var compiledOr = CelBuilder.celBuilder(src)
                .validateInput(paths -> List.of("bad A", "bad B", "bad A"))
                .validateContext(paths -> List.of("bad C"))
                .compile();

        assertTrue(compiledOr.isError(), "Expected errors from validators");
        var errs = compiledOr.getErrors();
        assertTrue(errs.contains("bad A"));
        assertTrue(errs.contains("bad B"));
        assertTrue(errs.contains("bad C"));
    }

    @Test
    void compile_respects_custom_var_names() {
        var src = "ctx[\"user\"] == \"admin\" && inp[\"amount\"] > 100";

        var compiledOr = CelBuilder
                .celBuilder(src)
                .withVarNames("inp", "ctx")
                .compile();

        assertTrue(compiledOr.isValue(), () -> "Compile errors: " + compiledOr.getErrors());
        var usage = compiledOr.getValue().get().usage();
        assertTrue(usage.inputPaths().contains("amount"));
        assertTrue(usage.contextPaths().contains("user"));
    }

    @Test
    void compile_fails_on_bad_cel() {
        var badSrc = "context[\"user\"] == \"admin\" && input["; // parse error
        var compiledOr = CelBuilder.celBuilder(badSrc).compile();
        assertTrue(compiledOr.isError(), "Expected CEL compile error");
        assertTrue(compiledOr.getErrors().stream().anyMatch(s -> s.toLowerCase().contains("compile")));
    }

    @Test
    void result_coercer_applies() {
        var src = "1 + 1";

        var compiledOr = CelBuilder.celBuilder(src)
                .withResultCoercer(o -> {
                    if (o instanceof Number n) return n.intValue();
                    throw new IllegalArgumentException("Expected number");
                })
                .compile();

        assertTrue(compiledOr.isValue(), () -> "Compile errors: " + compiledOr.getErrors());
        var compiled = compiledOr.getValue().get();

        ErrorsOr<Object> res = compiled.executor().execute(Map.of(), Map.of());
        assertTrue(res.isValue(), () -> "Eval errors: " + res.getErrors());
        assertEquals(2, res.getValue().get());
    }
}
