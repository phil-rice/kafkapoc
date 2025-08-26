package com.example.kafkaconsumer;

import com.example.kafka.consumer.abstraction.Demand;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import java.util.HashSet;
import java.util.Set;

final class PauseResumeApplier {
    private final Set<TopicPartition> paused = new HashSet<>();

    void apply(KafkaConsumer<String,String> consumer,
               Set<TopicPartition> assigned,
               Demand<TopicPartition> demand) {

        // desiredPaused = all assigned partitions with no credit
        Set<TopicPartition> desiredPaused = new HashSet<>();
        for (TopicPartition tp : assigned) {
            if (demand.allowance(tp) <= 0) {
                desiredPaused.add(tp);
            }
        }

        // compute deltas
        Set<TopicPartition> toPause  = new HashSet<>(desiredPaused);
        toPause.removeAll(paused);

        Set<TopicPartition> toResume = new HashSet<>(paused);
        toResume.removeAll(desiredPaused);

        if (!toPause.isEmpty())   consumer.pause(toPause);
        if (!toResume.isEmpty())  consumer.resume(toResume);

        // update state
        paused.clear();
        paused.addAll(desiredPaused);
    }

    void clear() { paused.clear(); }
}
