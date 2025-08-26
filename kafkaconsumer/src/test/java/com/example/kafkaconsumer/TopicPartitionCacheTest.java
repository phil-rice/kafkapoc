package com.example.kafkaconsumer;


import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class TopicPartitionCacheTest {

    private static TopicPartition tp(String topic, int p) {
        return new TopicPartition(topic, p);
    }

    @Test
    void emptyCache_fallbackReturnsNewInstanceEachTime() {
        TopicPartitionCache cache = TopicPartitionCache.empty();

        TopicPartition a = cache.get("t", 0);
        TopicPartition b = cache.get("t", 0);

        assertEquals("t", a.topic());
        assertEquals(0, a.partition());
        assertEquals("t", b.topic());
        assertEquals(0, b.partition());
        // empty cache uses fallback: each call returns a new instance
        assertNotSame(a, b, "empty() should not reuse instances for unknown topic/partition");
    }

    @Test
    void fromAssigned_reusesInstancesForInRangePartitions() {
        TopicPartitionCache cache = TopicPartitionCache.fromAssigned(List.of(
                tp("t1", 0),
                tp("t1", 1),
                tp("t2", 2)   // implies t2 will build 0..2
        ));

        // In-range lookups should return the SAME instance on repeat calls
        TopicPartition t1p0_a = cache.get("t1", 0);
        TopicPartition t1p0_b = cache.get("t1", 0);
        assertSame(t1p0_a, t1p0_b, "in-range lookups should reuse the cached TopicPartition");

        TopicPartition t1p1_a = cache.get("t1", 1);
        TopicPartition t1p1_b = cache.get("t1", 1);
        assertSame(t1p1_a, t1p1_b);

        // For t2, we only assigned partition 2, but cache should build 0..2
        TopicPartition t2p1_a = cache.get("t2", 1);
        TopicPartition t2p1_b = cache.get("t2", 1);
        assertSame(t2p1_a, t2p1_b, "cache should prebuild up to max partition and reuse instances");
    }

    @Test
    void outOfRangePartition_usesFallback_newInstanceEachCall() {
        TopicPartitionCache cache = TopicPartitionCache.fromAssigned(List.of(
                tp("t", 0), tp("t", 1) // builds 0..1
        ));

        TopicPartition a = cache.get("t", 5); // out of range -> fallback
        TopicPartition b = cache.get("t", 5); // fallback again -> new instance

        assertEquals("t", a.topic());
        assertEquals(5, a.partition());
        assertEquals("t", b.topic());
        assertEquals(5, b.partition());
        assertNotSame(a, b, "fallback path should return a fresh instance each call");
    }

    @Test
    void unknownTopic_usesFallback_newInstanceEachCall() {
        TopicPartitionCache cache = TopicPartitionCache.fromAssigned(List.of(
                tp("known", 0), tp("known", 1)
        ));

        TopicPartition a = cache.get("unknown", 0);
        TopicPartition b = cache.get("unknown", 0);

        assertEquals("unknown", a.topic());
        assertEquals(0, a.partition());
        assertNotSame(a, b, "unknown topic should use fallback each time");
    }

    @Test
    void negativePartition_usesFallback_newInstanceEachCall() {
        TopicPartitionCache cache = TopicPartitionCache.fromAssigned(List.of(
                tp("t", 0)
        ));

        TopicPartition a = cache.get("t", -1);
        TopicPartition b = cache.get("t", -1);

        assertEquals("t", a.topic());
        assertEquals(-1, a.partition());
        assertNotSame(a, b, "negative partition should use fallback each time");
    }

    @Test
    void nullOrEmptyAssigned_buildsEmptyCache() {
        // null -> empty cache
        TopicPartitionCache cacheNull = TopicPartitionCache.fromAssigned(null);
        TopicPartition x1 = cacheNull.get("t", 0);
        TopicPartition x2 = cacheNull.get("t", 0);
        assertNotSame(x1, x2);

        // empty list -> empty cache
        TopicPartitionCache cacheEmpty = TopicPartitionCache.fromAssigned(List.of());
        TopicPartition y1 = cacheEmpty.get("t", 0);
        TopicPartition y2 = cacheEmpty.get("t", 0);
        assertNotSame(y1, y2);
    }
}
