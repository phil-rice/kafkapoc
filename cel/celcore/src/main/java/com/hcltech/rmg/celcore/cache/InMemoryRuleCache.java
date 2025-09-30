package com.hcltech.rmg.celcore.cache;

import com.hcltech.rmg.celcore.CompiledRule;
import com.hcltech.rmg.common.errorsor.ErrorsOr;

import java.util.Collection;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

/**
 * Simple concurrent cache of compiled rules.
 * - One compile per key, result cached (success or failure).
 * - ConcurrentHashMap ensures thread safety.
 * - No eviction (unbounded, by design).
 */
public final class InMemoryRuleCache<Inp, Out> implements RuleCache<Inp, Out> {

    private final ConcurrentHashMap<String, ErrorsOr<CompiledRule<Inp, Out>>> cache = new ConcurrentHashMap<>();
    private final Function<String, ErrorsOr<CompiledRule<Inp, Out>>> compileFn;

    public InMemoryRuleCache(Function<String, ErrorsOr<CompiledRule<Inp, Out>>> compileFn) {
        this.compileFn = Objects.requireNonNull(compileFn, "compileFn");
    }

    @Override
    public ErrorsOr<CompiledRule<Inp, Out>> get(String key) {
        Objects.requireNonNull(key, "key");
        return cache.computeIfAbsent(key, compileFn);
    }


    public InMemoryRuleCache<Inp, Out> preloadWith(Collection<String> keys) {
        keys.forEach(k -> cache.computeIfAbsent(k, compileFn));
        return this;
    }

}
