package com.example.kafka.tools;

import java.util.List;
import java.util.SplittableRandom;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicLongArray;
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
    public Stream<TestDomainMessage> randomUniformStream(int domainCount) {
        if (domainCount <= 0) throw new IllegalArgumentException("domainCount must be > 0");
        var domainIds = IntStream.range(0, domainCount)
                .mapToObj(i -> "d-" + i)
                .toList();
        return randomUniformStream(domainIds);
    }

    /**
     * Infinite stream, uniform random selection across provided domains. Offsets are per-domain monotonic.
     */
    public Stream<TestDomainMessage> randomUniformStream(List<String> domainIds) {
        if (domainIds == null || domainIds.isEmpty()) throw new IllegalArgumentException("domainIds empty");
        var offsets = new AtomicLongArray(domainIds.size()); // starts at 0
        var rng = new SplittableRandom(seed);

        Supplier<TestDomainMessage> next = () -> {
            // pick a domain uniformly
            int idx = rng.nextInt(domainIds.size());
            long off = offsets.getAndIncrement(idx); // 0,1,2,... per domain
            return new TestDomainMessage(domainIds.get(idx), counter.getAndIncrement());
        };

        // Stream.generate is safe here because our supplier is thread-safe.
        // (Note: generate() doesn't parallelize; if you need parallel production,
        // just call next.get() from your own executor.)
        return Stream.generate(next);
    }

    /**
     * Finite batch helper (handy for tests).
     */
    public Stream<TestDomainMessage> randomUniformBatch(int domainCount, long count) {
        return randomUniformStream(domainCount).limit(count);
    }

    private DomainMessageGenerator() {
    }
}
