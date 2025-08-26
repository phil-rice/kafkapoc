// com.example.kafkaconsumer.LoggingSeekStrategy
package com.example.kafkaconsumer;

import com.example.kafka.consumer.abstraction.SeekOps;
import com.example.kafka.consumer.abstraction.SeekStrategy;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;

public final class LoggingSeekStrategy implements SeekStrategy<TopicPartition, Long> {
    private static final Logger log = LoggerFactory.getLogger(LoggingSeekStrategy.class);
    private final SeekStrategy<TopicPartition, Long> delegate;

    public LoggingSeekStrategy(SeekStrategy<TopicPartition, Long> delegate) {
        this.delegate = delegate;
    }

    @Override
    public void onAssigned(SeekOps<TopicPartition, Long> ops, Set<TopicPartition> shards) {
        ops.capability(KafkaOffsetIntrospector.class).ifPresent(oi -> {
            var begins = oi.beginningOffsets(shards);
            var ends   = oi.endOffsets(shards);
            long total = 0L;
            for (TopicPartition tp : shards) {
                long b = begins.getOrDefault(tp, 0L);
                long e = ends.getOrDefault(tp, 0L);
                long n = Math.max(0L, e - b);
                total += n;
                log.info("ASSIGNED {} start={} end={} count={}", tp, b, e, n);
            }
            log.info("ASSIGNED TOTAL count={}", total);
        });

        delegate.onAssigned(ops, shards);
    }
}
