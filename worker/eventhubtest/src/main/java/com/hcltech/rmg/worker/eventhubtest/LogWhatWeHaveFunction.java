package com.hcltech.rmg.worker.eventhubtest;

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

public class LogWhatWeHaveFunction<T> extends RichMapFunction<T, T> {
    private static final Logger log = LoggerFactory.getLogger(LogWhatWeHaveFunction.class);

    // Shared across all parallel subtasks in the same JVM
    private static final AtomicLong total = new AtomicLong(0);

    // Singleton logger thread state (shared across subtasks)
    private static final AtomicLong activeInstances = new AtomicLong(0);
    private static final AtomicBoolean loggingRunning = new AtomicBoolean(false);
    private static volatile Thread logThread = null;

    private static final long LOG_INTERVAL_MS = 2_000; // every 2 seconds

    private static ConcurrentHashMap<Thread,Thread> threadMemory = new ConcurrentHashMap<>();
    @Override
    public void open(OpenContext parameters) {
        long count = activeInstances.incrementAndGet();
        threadMemory.computeIfAbsent(Thread.currentThread(), k -> Thread.currentThread());

        // Start the singleton logger thread exactly once per JVM
        if (loggingRunning.compareAndSet(false, true)) {
            log.info("Starting singleton logging thread (first active instance: {})", count);

            logThread = new Thread(() -> {
                long lastCount = total.get();
                long lastTime = System.currentTimeMillis();

                while (loggingRunning.get()) {
                    try {
                        Thread.sleep(LOG_INTERVAL_MS);
                    } catch (InterruptedException ie) {
                        // Exit if asked to stop
                        Thread.currentThread().interrupt();
                        break;
                    }

                    long now = System.currentTimeMillis();
                    long c = total.get();
                    long deltaC = c - lastCount;
                    long deltaT = Math.max(1, now - lastTime);
                    double instRate = (deltaC * 1000.0) / deltaT;

                    log.info("Processed {} (+{}) in {} ms ({} rec/s inst). Threads {}",
                            c, deltaC, deltaT, String.format("%.0f", instRate), threadMemory.size());

                    lastCount = c;
                    lastTime = now;
                }

                log.info("Singleton logging thread exiting");
            }, "LogWhatWeHave-SingletonLogger");

            logThread.setDaemon(true);
            logThread.start();
        }
    }

    @Override
    public T map(T value) {
        total.incrementAndGet();
        return value;
    }

    @Override
    public void close() {
        long remaining = activeInstances.decrementAndGet();

        // Only stop the logger thread when the LAST subtask in this JVM closes
        if (remaining == 0) {
            if (loggingRunning.compareAndSet(true, false)) {
                Thread t = logThread;
                if (t != null) {
                    t.interrupt();
                    try {
                        t.join(1000);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    } finally {
                        logThread = null;
                    }
                }
                log.info("Stopped singleton logging thread (no active instances)");
            }
        } else {
            log.debug("Subtask closed; {} instances remain, logger stays running", remaining);
        }
    }
}
