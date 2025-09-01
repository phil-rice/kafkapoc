// com.example.kafka.consumer.abstraction.OffsetLoggingSeekStrategy.java
package com.hcltech.rmg.consumer.abstraction;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;

public final class OffsetLoggingSeekStrategy<S, P> implements SeekStrategy<S, P> {
    private static final Logger log = LoggerFactory.getLogger(OffsetLoggingSeekStrategy.class);
    private final SeekStrategy<S, P> delegate;

    public OffsetLoggingSeekStrategy(SeekStrategy<S, P> delegate) {
        this.delegate = delegate;
    }

    @Override
    public void onAssigned(SeekOps<S, P> ops, Set<S> shards) {
        ops.capability(OffsetIntrospector.class).ifPresent(oi -> {
            var begins = oi.beginningOffsets(shards);
            var ends   = oi.endOffsets(shards);

            long total = 0;
            for (S s : shards) {
                long b = ((Number) begins.getOrDefault(s, (P) (Long) 0L)).longValue();
                long e = ((Number) ends.getOrDefault(s, (P) (Long) 0L)).longValue();
                long n = Math.max(0, e - b);
                total += n;
                log.info("ASSIGN {} start={} end={} count={}", s, b, e, n);
            }
            log.info("ASSIGN TOTAL count={}", total);
        });

        delegate.onAssigned(ops, shards);
    }
}
