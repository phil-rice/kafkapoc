package com.hcltech.rmg.tools;

import java.util.List;
import java.util.SplittableRandom;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicLongArray;
import java.util.function.BiFunction;
import java.util.function.Supplier;
import java.util.stream.IntStream;
import java.util.stream.Stream;

public final class DomainMessageGenerator {


    public static DomainMessageGenerator withSeed(long seed) {
        return new DomainMessageGenerator(seed);
    }

    public long seed;
    public AtomicLong counter = new AtomicLong(0);

    public DomainMessageGenerator(long seed) {
        this.seed = seed;
    }

    /**
     * Infinite stream, uniform random selection across N domains. Offsets are per-domain monotonic.
     */
    public <T> Stream<T> randomUniformStream(int domainCount, BiFunction<String, Long, T> mapper) {
        if (domainCount <= 0) throw new IllegalArgumentException("domainCount must be > 0");
        var domainIds = IntStream.range(0, domainCount)
                .mapToObj(i -> "d-" + i)
                .toList();
        return randomUniformStream(domainIds, mapper);
    }

    /**
     * Infinite stream, uniform random selection across provided domains. Offsets are per-domain monotonic.
     */
    public <T> Stream<T> randomUniformStream(List<String> domainIds, BiFunction<String, Long, T> mapper) {
        if (domainIds == null || domainIds.isEmpty()) throw new IllegalArgumentException("domainIds empty");
        var offsets = new AtomicLongArray(domainIds.size()); // starts at 0
        var rng = new SplittableRandom(seed);

        Supplier<T> next = () -> {
            // pick a domain uniformly
            int idx = rng.nextInt(domainIds.size());
            return mapper.apply(idx + "", offsets.getAndIncrement(idx));
//            return new TestDomainMessage(domainIds.get(idx), counter.getAndIncrement());
        };

        // Stream.generate is safe here because our supplier is thread-safe.
        // (Note: generate() doesn't parallelize; if you need parallel production,
        // just call next.get() from your own executor.)
        return Stream.generate(next);
    }



}
