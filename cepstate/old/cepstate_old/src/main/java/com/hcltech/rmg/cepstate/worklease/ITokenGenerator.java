package com.hcltech.rmg.cepstate.worklease;

import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

public interface ITokenGenerator {
    String next(String domainId);

    static ITokenGenerator generator() {
        return (domainId) -> domainId + "-" + UUID.randomUUID();
    }

    static ITokenGenerator incrementingGenerator() {
        return new IncrementingTokenGenerator();
    }
}

final class IncrementingTokenGenerator implements ITokenGenerator {
    private final ConcurrentHashMap<String, AtomicLong> counters = new ConcurrentHashMap<>();

    @Override
    public String next(String domainId) {
        long n = counters.computeIfAbsent(domainId, k -> new AtomicLong()).incrementAndGet();
        return domainId + "-" + n;
    }
}