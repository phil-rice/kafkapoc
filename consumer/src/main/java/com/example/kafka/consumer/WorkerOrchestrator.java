package com.example.kafka.consumer;

import com.example.kafka.consumer.processors.RecordProcessor;
import com.example.kafka.consumer.processors.RecordProcessorFactory;
import com.example.metrics.MetricsRegistry;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/** Manages a dynamic set of consumer workers (subscribe-mode, same group.id). */
public class WorkerOrchestrator {
    private final AppConfig cfg;
    private final RecordProcessorFactory processorFactory;
    private final List<String> topics;            // all workers subscribe to same topics
    private final MetricsRegistry metrics;        // shared global metrics
    private final AtomicInteger seq = new AtomicInteger(0);

    /** id -> handle */
    private final Map<String, WorkerHandle> workers = new ConcurrentHashMap<>();

    public WorkerOrchestrator(AppConfig cfg,
                              RecordProcessorFactory factory,
                              List<String> topics,
                              MetricsRegistry metrics) {
        this.cfg = cfg;
        this.processorFactory = factory;
        this.topics = (topics == null || topics.isEmpty()) ? cfg.topics() : topics;
        this.metrics = metrics;
    }

    /** Start one new worker; returns its id. */
    public String startWorker() {
        String suffix = "w-" + seq.incrementAndGet();
        RecordProcessor rp = processorFactory.create();
        ConsumerRunner runner = new ConsumerRunner(cfg, rp, topics, suffix, metrics);
        Thread t = new Thread(runner, "consumer-" + suffix);
        t.start();
        workers.put(suffix, new WorkerHandle(suffix, runner, t));
        return suffix;
    }

    /** Stop a specific worker by id; returns true if it existed. */
    public boolean stopWorker(String id) {
        WorkerHandle h = workers.remove(id);
        if (h == null) return false;
        h.runner.requestShutdown();
        joinQuietly(h.thread, 15_000);
        return true;
    }

    /** Ensure we have exactly desired workers (scale up/down). */
    public void setDesiredWorkers(int desired) {
        int current = workers.size();
        if (desired > current) {
            for (int i = 0; i < desired - current; i++) startWorker();
        } else if (desired < current) {
            // stop arbitrary workers (e.g., newest first)
            List<String> ids = new ArrayList<>(workers.keySet());
            ids.sort(Comparator.naturalOrder());
            Collections.reverse(ids);
            for (int i = 0; i < current - desired; i++) stopWorker(ids.get(i));
        }
    }

    /** Stop everything. */
    public void stopAll() {
        List<String> ids = new ArrayList<>(workers.keySet());
        for (String id : ids) stopWorker(id);
    }

    /** Snapshot of active worker ids. */
    public List<String> activeWorkers() {
        return new ArrayList<>(workers.keySet());
    }

    private static void joinQuietly(Thread t, long millis) {
        try { t.join(millis); } catch (InterruptedException ie) { Thread.currentThread().interrupt(); }
    }

    private static final class WorkerHandle {
        final String id;
        final ConsumerRunner runner;
        final Thread thread;
        WorkerHandle(String id, ConsumerRunner runner, Thread thread) {
            this.id = id; this.runner = runner; this.thread = thread;
        }
    }
}
