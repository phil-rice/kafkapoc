package com.hcltech.rmg.kafka;

import com.hcltech.rmg.common.errorsor.ErrorsOr;
import com.hcltech.rmg.kafkaconfig.KafkaConfig;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class KafkaTopicsTest {

    @Test
    @DisplayName("ensureTopics: skips topic management when Event Hub connection details are present")
    void ensureTopics_skipsForEventHub() {
        Properties extra = new Properties();
        extra.put("eventhub.connection.string", "Endpoint=sb://example.servicebus.windows.net/;EntityPath=test");

        KafkaConfig cfg = new KafkaConfig(
                "ignored:9092",
                "test",
                "group",
                1,
                "earliest",
                Duration.ofSeconds(30),
                extra
        );

        ErrorsOr<Boolean> result = KafkaTopics.ensureTopics(cfg, List.of("test"), 1, (short) 1);

        assertTrue(result.isValue(), "Expected ensureTopics to return a value for Event Hub configs");
        assertFalse(result.valueOrThrow(), "ensureTopics should report no topics created when skipping Event Hub management");
    }
}
