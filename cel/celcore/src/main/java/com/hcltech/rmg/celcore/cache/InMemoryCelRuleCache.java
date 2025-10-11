package com.hcltech.rmg.celcore.cache;

import com.hcltech.rmg.celcore.CompiledCelRule;
import com.hcltech.rmg.celcore.CompiledCelRuleWithDetails;
import com.hcltech.rmg.common.errorsor.ErrorsOr;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Function;

public final class InMemoryCelRuleCache<Inp, Out>
        implements PopulateCelRuleCache<Inp, Out> {

    private final ConcurrentMap<String, CompiledCelRule<Inp, Out>> byKey = new ConcurrentHashMap<>();
    private final Function<String, ErrorsOr<CompiledCelRuleWithDetails<Inp, Out>>> compileFn;
    private final boolean overwriteOnPopulate;

    public InMemoryCelRuleCache(
            Function<String, ErrorsOr<CompiledCelRuleWithDetails<Inp, Out>>> compileFn,
            boolean overwriteOnPopulate
    ) {
        this.compileFn = Objects.requireNonNull(compileFn);
        this.overwriteOnPopulate = overwriteOnPopulate;
    }

    @Override
    public CompiledCelRule<Inp, Out> get(String key) {
        var rule = byKey.get(Objects.requireNonNull(key));
        if (rule == null) {
            List<String> keys = new ArrayList<>(byKey.keySet());
            keys.sort(String::compareTo);
            throw new CelRuleNotFoundException("No compiled rule for key: " + key + " Legal keys: " + keys);
        }
        return rule;
    }

    @Override
    public ErrorsOr<CompiledCelRuleWithDetails<Inp, Out>> populate(String key, String source) {
        Objects.requireNonNull(key);
        Objects.requireNonNull(source);

        if (!overwriteOnPopulate) {
            var existing = byKey.get(key);
            if (existing != null) {
                @SuppressWarnings("unchecked")
                var withDetails = (CompiledCelRuleWithDetails<Inp, Out>) existing;
                return ErrorsOr.lift(withDetails);
            }
        }

        var compiledOr = compileFn.apply(source);
        if (compiledOr.isError()) return ErrorsOr.errors(compiledOr.getErrors());

        var compiled = compiledOr.valueOrThrow(); // WithDetails
        if (overwriteOnPopulate) {
            byKey.put(key, compiled);
            return ErrorsOr.lift(compiled);
        } else {
            byKey.putIfAbsent(key, compiled);
            @SuppressWarnings("unchecked")
            var stored = (CompiledCelRuleWithDetails<Inp, Out>) byKey.get(key);
            return ErrorsOr.lift(stored);
        }
    }
}
