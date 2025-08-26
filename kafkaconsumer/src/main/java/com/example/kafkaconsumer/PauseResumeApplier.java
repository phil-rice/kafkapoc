package com.example.kafkaconsumer;

import com.example.kafka.consumer.abstraction.Demand;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

/** Computes and applies pause/resume deltas from Demand. */
final class PauseResumeApplier {
    private final Set<TopicPartition> paused = new HashSet<>();

    void apply(KafkaConsumer<String, String> consumer,
               Collection<TopicPartition> assigned,
               Demand<TopicPartition> demand) {
        if (assigned == null || assigned.isEmpty()) return;

        Set<TopicPartition> toPause = null;
        Set<TopicPartition> toResume = null;

        for (TopicPartition tp : assigned) {
            int allow = demand.allowance(tp);
            if (allow <= 0) {
                if (!paused.contains(tp)) {
                    if (toPause == null) toPause = new HashSet<>();
                    toPause.add(tp);
                }
            } else {
                if (paused.contains(tp)) {
                    if (toResume == null) toResume = new HashSet<>();
                    toResume.add(tp);
                }
            }
        }

        if (toPause != null && !toPause.isEmpty()) {
            consumer.pause(toPause);
            paused.addAll(toPause);
        }
        if (toResume != null && !toResume.isEmpty()) {
            consumer.resume(toResume);
            paused.removeAll(toResume);
        }
    }

    void clear() { paused.clear(); }
}
