package com.example.kafkaconsumer;

import com.example.kafka.consumer.abstraction.AssignmentListener;
import com.example.kafka.consumer.abstraction.Demand;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.time.Duration;
import java.util.*;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.*;

class KafkaFlowControlledStreamTest {

    private static TopicPartition tp(String t, int p) { return new TopicPartition(t, p); }
    private static ConsumerRecord<String,String> rec(String t, int p, long off) {
        return new ConsumerRecord<>(t, p, off, null, "v"+off);
    }

    @Test
    void demandLimitsDelivery_pauseResumeApplied_commitAndCloseWork() {
        // Mock consumer
        KafkaConsumer<String,String> consumer = mock(KafkaConsumer.class);

        // Capture the rebalance listener
        ArgumentCaptor<ConsumerRebalanceListener> rblCap = ArgumentCaptor.forClass(ConsumerRebalanceListener.class);
        doNothing().when(consumer).subscribe(anyList(), rblCap.capture());

        KafkaFlowControlledStream src = new KafkaFlowControlledStream(consumer, 8);

        // Subscribe (single topic)
        src.subscribe("t", new AssignmentListener<>() {
            @Override public void onAssigned(Set<TopicPartition> shards) {}
            @Override public void onRevoked(Set<TopicPartition> shards) {}
        });

        // Simulate assignment of two partitions
        var t0 = tp("t", 0);
        var t1 = tp("t", 1);
        rblCap.getValue().onPartitionsAssigned(Arrays.asList(t0, t1));

        // First poll: allow t0=1, t1=0 -> expect pause(t1); poll returns both records (buffer trims)
        when(consumer.poll(any())).thenReturn(cr(rec("t",0,0), rec("t",1,0)));

        Map<TopicPartition,Integer> allow1 = Map.of(t0, 1, t1, 0);
        var out1 = toList(src.poll(Duration.ofMillis(10), Demand.of(allow1)));

        // Verify pause called for t1
        ArgumentCaptor<Collection<TopicPartition>> pauseCap = ArgumentCaptor.forClass(Collection.class);
        verify(consumer).pause(pauseCap.capture());
        assertEquals(Set.of(t1), new HashSet<>(pauseCap.getValue()));

        // Only t0 should be returned (t1 was paused; extra goes to buffer)
        assertEquals(1, out1.size());
        assertEquals(0, out1.get(0).partition());
        clearInvocations(consumer);

        // Second poll: allow both=1 -> expect resume(t1); poll returns empty but buffer has t1
        when(consumer.poll(any())).thenReturn(emptyRecs());
        Map<TopicPartition,Integer> allow2 = Map.of(t0, 1, t1, 1);
        var out2 = toList(src.poll(Duration.ofMillis(10), Demand.of(allow2)));

        ArgumentCaptor<Collection<TopicPartition>> resumeCap = ArgumentCaptor.forClass(Collection.class);
        verify(consumer).resume(resumeCap.capture());
        assertEquals(Set.of(t1), new HashSet<>(resumeCap.getValue()));

        assertEquals(1, out2.size());
        assertEquals(1, out2.get(0).partition());

        // Commit maps Long->OffsetAndMetadata
        Map<TopicPartition,Long> commits = Map.of(t0, 1L, t1, 2L);
        src.commit(commits);
        ArgumentCaptor<Map<TopicPartition, OffsetAndMetadata>> commitCap = ArgumentCaptor.forClass(Map.class);
        verify(consumer).commitSync(commitCap.capture());
        assertEquals(1L, commitCap.getValue().get(t0).offset());
        assertEquals(2L, commitCap.getValue().get(t1).offset());

        // Close closes consumer
        src.close();
        verify(consumer).close();
    }

    private static ConsumerRecords<String,String> cr(ConsumerRecord<String,String>... rs) {
        Map<TopicPartition, List<ConsumerRecord<String,String>>> m = new HashMap<>();
        for (var r : rs) {
            var tp = new TopicPartition(r.topic(), r.partition());
            m.computeIfAbsent(tp, __ -> new ArrayList<>()).add(r);
        }
        return new ConsumerRecords<>(m);
    }

    private static ConsumerRecords<String,String> emptyRecs() {
        return new ConsumerRecords<>(Map.of());
    }

    private static <T> List<T> toList(Iterable<T> it) {
        List<T> out = new ArrayList<>();
        for (T t : it) out.add(t);
        return out;
    }
}
