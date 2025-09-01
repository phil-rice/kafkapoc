package com.hcltech.rmg.kafkaconsumer;

import com.hcltech.rmg.consumer.abstraction.Demand;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class PartitionBufferStoreTest {

    private static TopicPartition tp(String t, int p) { return new TopicPartition(t, p); }
    private static ConsumerRecord<String,String> rec(String t, int p, long off, String k, String v) {
        return new ConsumerRecord<>(t, p, off, k, v);
    }

    private static ConsumerRecords<String,String> records(ConsumerRecord<String,String>... rs) {
        Map<TopicPartition, List<ConsumerRecord<String,String>>> map = new HashMap<>();
        for (var r : rs) {
            var tp = new TopicPartition(r.topic(), r.partition());
            map.computeIfAbsent(tp, __ -> new ArrayList<>()).add(r);
        }
        return new ConsumerRecords<>(map);
    }

    @Test
    void addAndDrainHonorsAllowancesAndGlobalMax() {
        PartitionBufferStore store = new PartitionBufferStore(16);
        var t0 = tp("topic", 0);
        var assigned = List.of(t0);
        store.onAssigned(assigned);

        TopicPartitionCache cache = TopicPartitionCache.fromAssigned(assigned);

        // add 3 records
        store.add(records(
                rec("topic", 0, 0, "k0", "v0"),
                rec("topic", 0, 1, "k1", "v1"),
                rec("topic", 0, 2, "k2", "v2")
        ), cache);

        // allowance=1 -> only first
        var out1 = store.drain(assigned, Demand.of(Map.of(t0, 1)));
        var it1 = out1.iterator();
        assertTrue(it1.hasNext());
        assertEquals(0L, it1.next().offset());
        assertFalse(it1.hasNext());

        // allowance=2, globalMax=1 -> only one returned (offset 1)
        var out2 = store.drain(assigned, Demand.of(Map.of(t0, 2), 1));
        var it2 = out2.iterator();
        assertTrue(it2.hasNext());
        assertEquals(1L, it2.next().offset());
        assertFalse(it2.hasNext());

        // drain remaining (offset 2)
        var out3 = store.drain(assigned, Demand.of(Map.of(t0, 10)));
        var it3 = out3.iterator();
        assertTrue(it3.hasNext());
        assertEquals(2L, it3.next().offset());
        assertFalse(it3.hasNext());
    }

    @Test
    void ringBufferCapsAndDropsOldestWhenFull() {
        // buffer capacity = 2
        PartitionBufferStore store = new PartitionBufferStore(2);
        var t0 = tp("topic", 0);
        var assigned = List.of(t0);
        store.onAssigned(assigned);
        TopicPartitionCache cache = TopicPartitionCache.fromAssigned(assigned);

        // Add 3 -> should keep the last 2 (offsets 1 and 2)
        store.add(records(
                rec("topic", 0, 0, "k0", "v0"),
                rec("topic", 0, 1, "k1", "v1"),
                rec("topic", 0, 2, "k2", "v2")
        ), cache);

        var out = store.drain(assigned, Demand.of(Map.of(t0, 10)));
        var offs = new ArrayList<Long>();
        for (var r : out) offs.add(r.offset());
        assertEquals(List.of(1L, 2L), offs);
    }
}
