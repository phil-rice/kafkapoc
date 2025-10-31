package com.hcltech.rmg.celimpl;

import com.hcltech.rmg.celcore.*;
import com.hcltech.rmg.common.errorsor.ErrorsOr;
import dev.cel.common.CelAbstractSyntaxTree;
import dev.cel.common.CelException;
import dev.cel.common.CelValidationException;
import dev.cel.common.types.CelType;
import dev.cel.common.types.SimpleType;
import dev.cel.compiler.CelCompiler;
import dev.cel.compiler.CelCompilerFactory;
import dev.cel.parser.CelStandardMacro;
import dev.cel.runtime.CelRuntime;
import dev.cel.runtime.CelRuntimeFactory;

import java.util.*;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * Default implementation of CelRuleBuilder using SimpleType + addVar(...).
 * Runtime-optimized: no per-exec clear(), no iterator allocation in hot path,
 * ThreadLocal activation map with pre-created nodes for declared vars.
 */
final class DefaultCelRuleBuilder<Inp, Out> implements CelRuleBuilder<Inp, Out> {

    private final String source;

    // Keep declared vars (type + getter) in insertion order.
    private record VarSpec<Inp>(CelVarType type, Function<Inp, Object> getter) {}
    private final LinkedHashMap<String, VarSpec<Inp>> vars = new LinkedHashMap<>();

    // Caller may override.
    private BiFunction<Inp, Object, Out> resultCoercer = (inp, v) -> (Out) v;

    // NOOP sentinel lets us avoid composing an extra layer when unused.
    @SuppressWarnings("rawtypes")
    private static final BiConsumer NOOP = (in, act) -> {};
    @SuppressWarnings("unchecked")
    private BiConsumer<Inp, Map<String, Object>> activationFiller =
            (BiConsumer<Inp, Map<String, Object>>) NOOP;

    // Reusable, thread-safe runtime
    private static final CelRuntime BASE_RUNTIME =
            CelRuntimeFactory.standardCelRuntimeBuilder().build();

    DefaultCelRuleBuilder(String source) {
        this.source = Objects.requireNonNull(source, "source");
    }

    @Override
    public CelRuleBuilder<Inp, Out> withVar(
            String name, CelVarType type, Function<Inp, Object> getter) {
        Objects.requireNonNull(name, "name");
        Objects.requireNonNull(type, "type");
        Objects.requireNonNull(getter, "getter");
        vars.put(name, new VarSpec<>(type, getter));
        return this;
    }

    @Override
    public CelRuleBuilder<Inp, Out> withResultCoercer(BiFunction<Inp, Object, Out> coercer) {
        this.resultCoercer = Objects.requireNonNull(coercer, "coercer");
        return this;
    }

    @Override
    public CelRuleBuilder<Inp, Out> withActivationFiller(
            BiConsumer<Inp, Map<String, Object>> filler) {
        this.activationFiller = Objects.requireNonNull(filler, "activationFiller");
        return this;
    }

    @Override
    public ErrorsOr<CompiledCelRuleWithDetails<Inp, Out>> compile() {
        final List<String> errors = new ArrayList<>();
        try {
            // 1) Build compiler and declare variables.
            var cb = CelCompilerFactory.standardCelCompilerBuilder().setStandardMacros(CelStandardMacro.HAS);
            for (var e : vars.entrySet()) {
                cb = cb.addVar(e.getKey(), toSimpleType(e.getValue().type()));
            }
            CelCompiler compiler = cb.build();

            // 2) Compile source → AST
            CelAbstractSyntaxTree ast = compiler.compile(source).getAst();

            // 3) Create reusable program
            CelRuntime.Program program = BASE_RUNTIME.createProgram(ast);

            // 4) Precompute hot-path arrays (avoid iterator allocs per exec)
            final int n = vars.size();
            final String[] varNames = new String[n];
            @SuppressWarnings("unchecked")
            final Function<Inp, Object>[] getters = new Function[n];

            int i = 0;
            for (var e : vars.entrySet()) {
                varNames[i] = e.getKey();
                getters[i] = e.getValue().getter();
                i++;
            }

            // 5) Precompute an initial capacity that avoids rehash for n keys
            final int initialCapacity = stableMapCapacity(n);

            // 6) Compose the single filler once:
            //    - We first populate all declared vars via getters for this input
            //    - Then (if provided) call user filler to override declared keys or add extras.
            //      (Note: extras will persist in the ThreadLocal map; CEL ignores undeclared keys.)
            final BiConsumer<Inp, Map<String, Object>> composedFiller;
            if (activationFiller == NOOP) {
                composedFiller = (in, act) -> {
                    for (int k = 0; k < n; k++) {
                        act.put(varNames[k], getters[k].apply(in));
                    }
                };
            } else {
                composedFiller = (in, act) -> {
                    for (int k = 0; k < n; k++) {
                        act.put(varNames[k], getters[k].apply(in));
                    }
                    activationFiller.accept(in, act); // may override declared keys
                };
            }

            // 7) Create executor with pre-populated per-thread map (no clear on execute)
            CelExecutor<Inp, Out> exec =
                    new ExecutorImpl(
                            source, program, composedFiller, resultCoercer, varNames, initialCapacity);

            // 8) Return compiled rule with details
            return ErrorsOr.lift(new CompiledWithDetails<>(source, exec));

        } catch (CelValidationException e) {
            errors.add("CEL compile/validate error: " + e.getMessage());
            return ErrorsOr.errors(errors);
        } catch (Exception e) {
            errors.add("CEL build error: " + e.getMessage());
            return ErrorsOr.errors(errors);
        }
    }

    private static CelType toSimpleType(CelVarType t) {
        return switch (t) {
            case DYN -> SimpleType.DYN;
            case STRING -> SimpleType.STRING;
            case INT64 -> SimpleType.INT; // CEL INT is 64-bit
            case DOUBLE -> SimpleType.DOUBLE;
            case BOOL -> SimpleType.BOOL;
        };
    }

    // Choose a capacity that won't resize for n inserts at default load factor (0.75)
    private static int stableMapCapacity(int n) {
        if (n <= 0) return 4;
        int cap = (int) Math.ceil(n / 0.75d);
        int highest = Integer.highestOneBit(cap - 1) << 1; // round up to power of 2
        return Math.max(4, highest);
    }

    /**
     * Hot-path executor:
     * - ThreadLocal HashMap pre-populated once with a Node per declared key (value initially null).
     * - On each execute(): overwrite values in place (no clear, no new Nodes, no rehash).
     */
    private final class ExecutorImpl implements CelExecutor<Inp, Out> {
        private final String src;
        private final CelRuntime.Program program;
        private final BiConsumer<Inp, Map<String, Object>> filler;
        private final BiFunction<Inp, Object, Out> coercer;

        // For pre-population
        private final String[] declaredNames;

        // One map per thread, created once with Nodes for all declared keys
        private final ThreadLocal<HashMap<String, Object>> tl;

        ExecutorImpl(
                String src,
                CelRuntime.Program program,
                BiConsumer<Inp, Map<String, Object>> filler,
                BiFunction<Inp, Object, Out> coercer,
                String[] declaredNames,
                int initialCapacity) {
            this.src = src;
            this.program = program;
            this.filler = filler;
            this.coercer = coercer;
            this.declaredNames = declaredNames;

            this.tl =
                    ThreadLocal.withInitial(
                            () -> {
                                HashMap<String, Object> m = new HashMap<>(initialCapacity);
                                // Allocate a Node per declared key once per thread.
                                for (String name : declaredNames) {
                                    m.put(name, null);
                                }
                                return m;
                            });
        }

        @Override
        public Out execute(Inp input) {
            try {
                HashMap<String, Object> act = tl.get();

                // Overwrite declared values in place — no clear(), no node allocation, no rehash.
                filler.accept(input, act);

                Object raw = program.eval(act);
                return coercer.apply(input, raw);

            } catch (CelException e) {
                throw new CelExecutionException(e);
            }
        }
    }

    /**
     * Minimal wrapper that exposes source() and pathsFor(varName) alongside the executor.
     */
    private record CompiledWithDetails<Inp, Out>(String source, CelExecutor<Inp, Out> executor)
            implements CompiledCelRuleWithDetails<Inp, Out> {

        @Override
        public String source() {
            return source;
        }

        @Override
        public List<String> pathsFor(String varName) {
            // If/when you add AST-based path extraction, return real paths here.
            return List.of();
        }

        @Override
        public CelExecutor<Inp, Out> executor() {
            return executor;
        }
    }
}
