package com.example.metrics;

import org.apache.kafka.common.TopicPartition;
import java.util.Map;

public record MetricsSnapshot(
        long timestampMs,
        long totalProcessed,
        Map<TopicPartition, Long> processedByPartition
) {}
