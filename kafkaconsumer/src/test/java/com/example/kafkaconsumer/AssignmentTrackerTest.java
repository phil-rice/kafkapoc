package com.example.kafkaconsumer;

import com.example.kafka.consumer.abstraction.AssignmentListener;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

class AssignmentTrackerTest {

    private static TopicPartition tp(String topic, int p) {
        return new TopicPartition(topic, p);
    }

    private static class CapturingListener implements AssignmentListener<TopicPartition> {
        Set<TopicPartition> lastAssigned = Set.of();
        Set<TopicPartition> lastRevoked  = Set.of();

        @Override
        public void onAssigned(Set<TopicPartition> shards) { lastAssigned = Set.copyOf(shards); }

        @Override
        public void onRevoked(Set<TopicPartition> shards) { lastRevoked = Set.copyOf(shards); }
    }

    @Test
    void assignedAndRevokedUpdateState_andListenerNotified_preservesOrder() {
        // Mocks & collaborators
        KafkaConsumer<String,String> consumer = Mockito.mock(KafkaConsumer.class);

        PartitionBufferStore buffers = new PartitionBufferStore(16);
        AssignmentTracker tracker = new AssignmentTracker(buffers);
        CapturingListener listener = new CapturingListener();

        var rbl = tracker.asRebalanceListener(consumer, listener);

        // Prepare a deterministic assignment order: t1-1, t1-0, t2-2
        TopicPartition t11 = tp("t1", 1);
        TopicPartition t10 = tp("t1", 0);
        TopicPartition t22 = tp("t2", 2);
        LinkedHashSet<TopicPartition> assignedOrder =
                new LinkedHashSet<>(Arrays.asList(t11, t10, t22));

        // ---- assign ----
        rbl.onPartitionsAssigned(assignedOrder);

        // Listener got exact set
        assertEquals(assignedOrder, listener.lastAssigned, "listener should receive assigned set");

        // Tracker state contains all and preserves insertion order
        assertEquals(3, tracker.assigned().size(), "assigned size");
        assertIterableEquals(assignedOrder, tracker.assigned(), "assigned order should be preserved");

        // TP cache should serve prebuilt instances for assigned partitions (same reference on repeat)
        TopicPartition c1 = tracker.tpCache().get("t1", 1);
        TopicPartition c1b = tracker.tpCache().get("t1", 1);
        assertSame(c1, c1b, "cache should reuse in-range TopicPartition instance");

        // ---- revoke one ----
        rbl.onPartitionsRevoked(Set.of(t10));

        // Listener got revoke set
        assertEquals(Set.of(t10), listener.lastRevoked, "listener should receive revoked set");

        // Tracker removed revoked partition; remaining order preserved
        assertEquals(2, tracker.assigned().size(), "assigned size after revoke");
        assertIterableEquals(new LinkedHashSet<>(Arrays.asList(t11, t22)), tracker.assigned(),
                "remaining assigned order should be preserved");

        // Cache is rebuilt; still returns reusable instances for remaining
        TopicPartition c2 = tracker.tpCache().get("t2", 2);
        TopicPartition c2b = tracker.tpCache().get("t2", 2);
        assertSame(c2, c2b, "cache should reuse instance for remaining assigned partition");

        // Clearing works
        tracker.clear();
        assertTrue(tracker.assigned().isEmpty(), "assigned should be empty after clear()");
        // cache now empty -> fallback path returns new instance each call
        TopicPartition x1 = tracker.tpCache().get("x", 0);
        TopicPartition x2 = tracker.tpCache().get("x", 0);
        assertNotSame(x1, x2, "empty cache uses fallback (new instance each call)");
    }
}
