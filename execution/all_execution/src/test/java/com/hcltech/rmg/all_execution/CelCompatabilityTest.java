package com.hcltech.rmg.all_execution;

import dev.cel.common.CelAbstractSyntaxTree;
import dev.cel.common.CelValidationException;
import dev.cel.common.types.SimpleType;
import dev.cel.compiler.CelCompiler;
import dev.cel.compiler.CelCompilerFactory;
import dev.cel.parser.CelStandardMacro;
import dev.cel.runtime.CelEvaluationException;
import dev.cel.runtime.CelRuntime;
import dev.cel.runtime.CelRuntimeFactory;
import org.junit.jupiter.api.Test;


import java.util.Map;

public class CelCompatabilityTest {

    private static final CelCompiler CEL_COMPILER =
            CelCompilerFactory.standardCelCompilerBuilder().setStandardMacros(CelStandardMacro.HAS).addVar("my_var", SimpleType.DYN).build();


    private static final CelRuntime CEL_RUNTIME =
            CelRuntimeFactory.standardCelRuntimeBuilder().build();

    @Test
    public void x() throws CelEvaluationException, CelValidationException {
        // Compile the expression into an Abstract Syntax Tree.
            CelAbstractSyntaxTree ast = CEL_COMPILER.compile("has(my_var.x)").getAst();

        // Plan an executable program instance.
        CelRuntime.Program program = CEL_RUNTIME.createProgram(ast);

        // Evaluate the program with an input variable.
        var result =  program.eval(Map.of("my_var", Map.of("x", "Hello World")));
        System.out.println(result); // 'Hello World!'
    }
}
