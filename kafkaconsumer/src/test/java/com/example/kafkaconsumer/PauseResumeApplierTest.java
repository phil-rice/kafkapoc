package com.example.kafkaconsumer;

import com.example.kafka.consumer.abstraction.Demand;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.anyCollection;
import static org.mockito.Mockito.*;

class PauseResumeApplierTest {

    private static TopicPartition tp(String t, int p) { return new TopicPartition(t, p); }

    @Test
    void appliesPauseAndResumeFromDemand_andIsIdempotent() {
        KafkaConsumer<String,String> consumer = mock(KafkaConsumer.class);
        PauseResumeApplier applier = new PauseResumeApplier();

        var t0 = tp("topic", 0);
        var t1 = tp("topic", 1);
        var assigned = List.of(t0, t1);

        // First: allow t0=1, t1=0 -> should pause t1
        var d1 = Demand.of(Map.of(t0, 1, t1, 0));
        applier.apply(consumer, assigned, d1);

        ArgumentCaptor<Set<TopicPartition>> pauseCap = ArgumentCaptor.forClass(Set.class);
        verify(consumer).pause(pauseCap.capture());
        assertEquals(Set.of(t1), pauseCap.getValue());

        // Re-applying same demand shouldn't call pause again (idempotent)
        clearInvocations(consumer);
        applier.apply(consumer, assigned, d1);
        verify(consumer, never()).pause(anyCollection());

        // Now: allow t0=1, t1=1 -> should resume t1 only
        var d2 = Demand.of(Map.of(t0, 1, t1, 1));
        applier.apply(consumer, assigned, d2);

        ArgumentCaptor<Set<TopicPartition>> resumeCap = ArgumentCaptor.forClass(Set.class);
        verify(consumer).resume(resumeCap.capture());
        assertEquals(Set.of(t1), resumeCap.getValue());

        // Re-applying same demand shouldn't call resume again
        clearInvocations(consumer);
        applier.apply(consumer, assigned, d2);
        verify(consumer, never()).resume(anyCollection());
    }
}
