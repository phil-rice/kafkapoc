package com.example.metrics;

import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Comparator;
import java.util.Map;
import java.util.stream.Collectors;

public class LoggingMetricsPrinter implements MetricsPrinter {
    private static final Logger log = LoggerFactory.getLogger(LoggingMetricsPrinter.class);

    @Override
    public void print(MetricsSnapshot s, long delta, double ratePerSec) {
        String perPartition = s.processedByPartition().entrySet().stream()
                .sorted(Comparator.comparing((Map.Entry<TopicPartition, Long> e) -> e.getKey().topic())
                        .thenComparing(e -> e.getKey().partition()))
                .map(e -> e.getKey().topic() + "-" + e.getKey().partition() + ":" + e.getValue())
                .collect(Collectors.joining(", "));
        log.info("METRICS totalProcessed={} (+{}, ~{} msg/s) perPartition=[{}]",
                s.totalProcessed(), delta, Math.round(ratePerSec), perPartition);
    }
}
