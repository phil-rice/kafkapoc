package com.example.kafka.consumer;

import com.example.kafka.consumer.processors.RecordProcessorFactory;
import com.example.kafka.consumer.processors.SimpleRecordProcessor;
import com.example.kafka.consumer.processors.SimulatedDelayRecordProcessor;
import com.example.metrics.LoggingMetricsPrinter;
import com.example.metrics.MetricsRegistry;
import com.example.metrics.MetricsScheduler;

public class Main {
    public static void main(String[] args) {
        AppConfig cfg = AppConfig.load();

        // 1) Shared metrics registry + scheduler (single tick thread)
        MetricsRegistry metrics = new MetricsRegistry();
        var printer = new LoggingMetricsPrinter();
        var scheduler = new MetricsScheduler(metrics, printer, cfg.metricsTickMs());
        scheduler.start();


        RecordProcessorFactory factory = () -> new SimulatedDelayRecordProcessor(cfg.processorDelayMs());
//        RecordProcessorFactory factory = SimpleRecordProcessor::new;


        WorkerOrchestrator orch = new WorkerOrchestrator(cfg, factory, cfg.topics(), metrics);

        // Adjust this as you like (or make it a config property)
        orch.setDesiredWorkers(3);

        // 3) Graceful shutdown
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            orch.stopAll();
            try { scheduler.close(); } catch (Exception ignored) {}
        }, "orchestrator-shutdown"));
    }
}
