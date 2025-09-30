package com.hcltech.rmg.celimpl;

import com.hcltech.rmg.celcore.*;
import com.hcltech.rmg.common.errorsor.ErrorsOr;
import dev.cel.common.CelAbstractSyntaxTree;
import dev.cel.common.CelValidationException;
import dev.cel.common.types.SimpleType;
import dev.cel.compiler.CelCompiler;
import dev.cel.compiler.CelCompilerFactory;
import dev.cel.runtime.CelRuntime;
import dev.cel.runtime.CelRuntimeFactory;

import java.util.*;
import java.util.function.Function;

/**
 * CEL-backed adapter implementing the hexagonal RuleBuilder port.
 * <p>
 * Notes:
 * - Integers in activation maps must be Long (CEL int is 64-bit).
 * - Prefer bracket access: input["field"], context["field"].
 */
public final class CelBuilder<Inp, Out> implements RuleBuilder<Inp, Out> {

    /* ------------------------- Configurable knobs ------------------------- */

    private final String source;

    private String inputVar = "input";
    private String contextVar = "context";

    // Optional result coercion (e.g., Boolean)
    private Function<Object, Object> resultCoercer = o -> o;

    // Engine-specific (optional) AST validators – kept private to adapter
    private final List<Function<CelAbstractSyntaxTree, List<String>>> astValidators = new ArrayList<>();

    // Engine-agnostic validators exposed via the port
    private final List<Function<List<String>, List<String>>> inputValidators = new ArrayList<>();
    private final List<Function<List<String>, List<String>>> contextValidators = new ArrayList<>();

    private PathExtractor pathExtractor = new DefaultPathExtractor();

    /* ------------------------- Shared runtime ------------------------- */

    private static final CelRuntime BASE_RUNTIME =
            CelRuntimeFactory.standardCelRuntimeBuilder().build();

    private CelBuilder(String source) {
        this.source = Objects.requireNonNull(source, "source");
    }

    public static <Inp, Out> CelBuilder<Inp, Out> celBuilder(String sourceCode) {
        return new CelBuilder<>(sourceCode);
    }

    public static RuleBuilderFactory celFactory() {
        return CelBuilder::celBuilder;
    }

    /* ------------------------- CEL-specific extras (optional) ------------------------- */

    /**
     * Add an AST-level validator (adapter-private, not in hex port).
     */
    public CelBuilder<Inp, Out> validate(Function<CelAbstractSyntaxTree, List<String>> validator) {
        this.astValidators.add(Objects.requireNonNull(validator, "validator"));
        return this;
    }

    /**
     * Swap the default (regex) extractor for a proper CEL-AST visitor when you’re ready.
     */
    public CelBuilder<Inp, Out> withPathExtractor(PathExtractor extractor) {
        this.pathExtractor = Objects.requireNonNull(extractor, "extractor");
        return this;
    }

    /* ------------------------- RuleBuilder (hex port) ------------------------- */

    @Override
    public CelBuilder<Inp, Out> validateInput(Function<List<String>, List<String>> validator) {
        this.inputValidators.add(Objects.requireNonNull(validator, "validator"));
        return this;
    }

    @Override
    public CelBuilder<Inp, Out> validateContext(Function<List<String>, List<String>> validator) {
        this.contextValidators.add(Objects.requireNonNull(validator, "validator"));
        return this;
    }

    @Override
    public CelBuilder<Inp, Out> withVarNames(String inputVar, String contextVar) {
        this.inputVar = Objects.requireNonNull(inputVar, "inputVar");
        this.contextVar = Objects.requireNonNull(contextVar, "contextVar");
        return this;
    }

    @Override
    public CelBuilder<Inp, Out> withResultCoercer(Function<Object, Object> coercer) {
        this.resultCoercer = Objects.requireNonNull(coercer, "coercer");
        return this;
    }

    @Override
    public ErrorsOr<CompiledRule<Inp, Out>> compile() {
        final List<String> errors = new ArrayList<>();
        try {
            // Build compiler with configured var names
            final CelCompiler compiler =
                    CelCompilerFactory.standardCelCompilerBuilder()
                            .addVar(contextVar, SimpleType.DYN)
                            .addVar(inputVar, SimpleType.DYN)
                            .build();

            final CelRuntime runtime = BASE_RUNTIME;

            // 1) compile -> AST (parse + check)
            final CelAbstractSyntaxTree ast = compiler.compile(source).getAst();

            // 2) optional AST validators (adapter-private)
            for (var v : astValidators) addDistinct(errors, v.apply(ast));

            // 3) extract usage & run input/context validators
            final Usage usage = pathExtractor.extractUsage(ast, source, inputVar, contextVar);
            for (var v : inputValidators) addDistinct(errors, v.apply(usage.inputPaths()));
            for (var v : contextValidators) addDistinct(errors, v.apply(usage.contextPaths()));

            if (!errors.isEmpty()) return ErrorsOr.errors(errors);

            // 4) program + executor
            final CelRuntime.Program program = runtime.createProgram(ast);

            RuleExecutor<Inp, Out> executor = (input, context) -> {
                try {
                    Map<String, Object> activation = new HashMap<>(2);
                    activation.put(inputVar, input);
                    activation.put(contextVar, context);
                    Object raw = program.eval(activation);
                    Out coerced = (Out) resultCoercer.apply(raw);
                    return ErrorsOr.lift(coerced);
                } catch (Exception e) {
                    return ErrorsOr.error("Evaluation error: " + e.getClass().getSimpleName() + ": " + e.getMessage());
                }
            };

            return ErrorsOr.lift(new Compiled<>(source, usage, executor));

        } catch (CelValidationException e) {
            errors.add("CEL compile/validate error: " + e.getMessage());
            return ErrorsOr.errors(errors);
        } catch (Exception e) {
            errors.add("CEL build error: " + e.getMessage());
            return ErrorsOr.errors(errors);
        }
    }

    private static void addDistinct(List<String> acc, List<String> maybe) {
        if (maybe != null && !maybe.isEmpty()) {
            maybe.stream().filter(Objects::nonNull).distinct().forEach(acc::add);
        }
    }

    /* ------------------------- SPI for path extraction ------------------------- */

    public interface PathExtractor {
        Usage extractUsage(CelAbstractSyntaxTree ast, String rawSource, String inputVar, String contextVar);
    }

    public record Usage(List<String> inputPaths, List<String> contextPaths) implements RuleUsage {
    }

    static final class DefaultPathExtractor implements PathExtractor {
        @Override
        public Usage extractUsage(CelAbstractSyntaxTree ast, String raw, String inputVar, String contextVar) {
            Set<String> input = new LinkedHashSet<>();
            Set<String> ctx = new LinkedHashSet<>();

            String dotted = "\\b(" + Pattern.quote(inputVar) + "|" + Pattern.quote(contextVar) + ")\\s*\\.\\s*([A-Za-z_][A-Za-z0-9_]*(?:\\s*\\.\\s*[A-Za-z_][A-Za-z0-9_]*)*)";
            String bracket = "\\b(" + Pattern.quote(inputVar) + "|" + Pattern.quote(contextVar) + ")(?:\\s*\\[\\s*([\"'])([^\"']+)\\2\\s*\\])+(?!\\s*\\[)";

            var DOTTED = java.util.regex.Pattern.compile(dotted);
            var BRACKET = java.util.regex.Pattern.compile(bracket);

            var md = DOTTED.matcher(raw);
            while (md.find()) {
                String root = md.group(1);
                String tail = md.group(2).replaceAll("\\s*", "");
                (root.equals(inputVar) ? input : ctx).add(tail);
            }

            var mb = BRACKET.matcher(raw);
            while (mb.find()) {
                String root = mb.group(1);
                String chain = mb.group(0);
                var keyMatcher = java.util.regex.Pattern.compile("\\[\\s*([\"'])([^\"']+)\\1\\s*\\]").matcher(chain);
                List<String> parts = new ArrayList<>();
                while (keyMatcher.find()) parts.add(keyMatcher.group(2));
                if (!parts.isEmpty()) (root.equals(inputVar) ? input : ctx).add(String.join(".", parts));
            }

            return new Usage(List.copyOf(input), List.copyOf(ctx));
        }
    }

    /* ------------------------- Helpers ------------------------- */

    // Local alias to avoid importing java.util.regex.Pattern at top level.
    private static final class Pattern {
        static String quote(String s) {
            return java.util.regex.Pattern.quote(s);
        }
    }

    /* ------------------------- Wire records to hex interfaces ------------------------- */

    public record Compiled<Inp, Out>(
            String source,
            RuleUsage usage,
            RuleExecutor<Inp, Out> executor
    ) implements CompiledRule<Inp, Out> {

    }
}
