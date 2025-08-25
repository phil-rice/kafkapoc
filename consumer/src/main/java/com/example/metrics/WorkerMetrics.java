package com.example.metrics;

import org.apache.kafka.common.TopicPartition;

import java.util.List;
import java.util.Map;

public record WorkerMetrics(
        String workerId,
        long timestampMs,
        long totalProcessed,
        long processedDelta,
        double processedPerSec,
        Map<TopicPartition, Long> processedByPartition,
        List<TopicPartition> assigned
) {}
