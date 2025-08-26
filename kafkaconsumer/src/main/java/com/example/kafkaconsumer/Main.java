package com.example.kafkaconsumer;

import com.example.kafka.consumer.abstraction.*;

import com.example.kafka.consumer.processors.SimpleMessageProcessor;
import com.example.kafka.consumer.processors.SimulatedDelayMessageProcessor;
import com.example.metrics.LoggingMetricsPrinter;
import com.example.metrics.MetricsPrinter;
import com.example.metrics.MetricsRegistry;
import com.example.metrics.MetricsScheduler;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import java.util.Set;
import java.util.concurrent.Executors;

public class Main {
    public static void main(String[] args) {
        // 1) Load config
        AppConfig cfg = AppConfig.load();

        // 2) Kafka consumer + flow-controlled stream
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(cfg.kafkaProperties());
        FlowControlledStream<ConsumerRecord<String, String>, TopicPartition, Long> stream =
                new KafkaFlowControlledStream(consumer, cfg.runnerBufferCapacity());

        // 3) RecordAccess + NextPosition
        var access = new KafkaRecordAccess();

        NextPosition<ConsumerRecord<String, String>, Long> nextPos = r -> r.offset() + 1;

        // 4) Business logic
//        MessageProcessor<ConsumerRecord<String, String>, TopicPartition> processor = new SimpleMessageProcessor();
        MessageProcessor<ConsumerRecord<String, String>, TopicPartition> processor = new SimulatedDelayMessageProcessor(cfg.simulatedProcessingDelayMs());

        // 5) Seek plumbing
//        SeekStrategy<TopicPartition, Long> seekStrategy = cfg.seekStrategy();
        SeekStrategy<TopicPartition, Long> seekStrategy =
                new LoggingSeekStrategy(cfg.seekStrategy());

        var seekOps = new KafkaSeekOps(consumer);

        // 6) Metrics
        MetricsRegistry<TopicPartition> registry = new MetricsRegistry<>();
        MetricsPrinter<TopicPartition> printer = new LoggingMetricsPrinter<>();
        try (MetricsScheduler<TopicPartition> scheduler = new MetricsScheduler<>(registry, printer, 2000)) {
            scheduler.start();
            System.out.println("Metrics scheduler started");

            // 7) Worker (make sure your GenericWorker calls seekStrategy.onAssigned(seekOps, shards) in onAssigned)
            GenericWorker<ConsumerRecord<String, String>, TopicPartition, Long> worker =
                    new GenericWorker<>(
                            cfg.topic(),
                            stream,
                            access,
                            processor,
                            nextPos,
                            registry,                 // if your GenericWorker takes a registry; otherwise remove
                            seekStrategy,             // NEW: inject strategy
                            seekOps,                  // NEW: inject ops
                            cfg.pollMs(),
                            cfg.commitTickMs(),
                            Executors.defaultThreadFactory(),
                            cfg.runnerBufferCapacity()
                    );

            // 8) Run
            Thread t = new Thread(worker, "worker-" + cfg.clientId());
            t.start();

            // 9) Shutdown
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                System.out.println("Shutdown requested");
                try {
                    scheduler.close();
                } catch (Exception ignored) {
                }
                worker.requestStop();
                try {
                    t.join();
                } catch (InterruptedException ignored) {
                }
            }));
            try {
                t.join();                 // keep app running until worker stops (Ctrl+C)
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
            }
        }
    }
}
