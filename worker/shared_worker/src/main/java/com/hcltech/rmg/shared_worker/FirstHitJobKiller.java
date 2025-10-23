package com.hcltech.rmg.shared_worker;


import org.apache.flink.core.execution.JobClient;

import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Cancels the Flink job when a trigger flips to true.
 */
public final class FirstHitJobKiller implements AutoCloseable {
    private final JobClient jobClient;
    private final long periodMillis;

    private final AtomicBoolean once;
    private final AtomicBoolean nowKillIt = new AtomicBoolean(false);
    private final ScheduledExecutorService scheduler;
    private ScheduledFuture<?> task;

    public FirstHitJobKiller(JobClient jobClient, AtomicBoolean once, Duration pollInterval) {
        this.jobClient = Objects.requireNonNull(jobClient, "jobClient");
        this.periodMillis = Math.max(1, pollInterval.toMillis());
        this.once = once;
        this.scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "first-hit-job-killer");
            t.setDaemon(true);
            return t;
        });
    }

    public synchronized void start() {
        if (task != null && !task.isCancelled()) return;
        task = scheduler.scheduleAtFixedRate(this::tick, 0, periodMillis, TimeUnit.MILLISECONDS);
    }

    private void tick() {
        if (!once.get())return ;// not found anything yet
        if (nowKillIt.compareAndSet(false, true)) {
            try {
                jobClient.cancel().get(); // fast stop; fine for AT_LEAST_ONCE
            } catch (Exception e) {
                e.printStackTrace();      // or your logger
            } finally {
                close();
            }
        }
    }

    @Override
    public synchronized void close() {
        if (task != null) task.cancel(true);
        scheduler.shutdownNow();
    }
}
