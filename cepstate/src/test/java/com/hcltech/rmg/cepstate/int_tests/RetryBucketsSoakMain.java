package com.hcltech.rmg.cepstate.int_tests;

import com.hcltech.rmg.common.ITimeService;
import com.hcltech.rmg.cepstate.metrics.IMetrics;
import com.hcltech.rmg.cepstate.retry.BucketRunner;
import com.hcltech.rmg.cepstate.retry.RetryBuckets;
import com.hcltech.rmg.cepstate.worklease.WorkLeaseStage;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.*;
import java.util.concurrent.atomic.LongAdder;

public class RetryBucketsSoakMain {

    // ==========================
    // TUNABLE PARAMETERS
    // ==========================
    static final int DURATION_SECONDS = 5;    // run generator for this many seconds
    static final int EVENTS_PER_SECOND = 200_000;    // new items per second
    static final int FAILURE_ONE_IN = 10;     // 1 in N fails
    static final long GRANULARITY_MS = 100;    // bucket width and tick rate
    static final int[] RETRY_TIMEOUTS_MS = {0, 500, 1_000, 2_000}; // ladder: 1s, then 2s, then give up
    static final int JITTER_MS = 100;      // keep deterministic
    static final long POST_DRAIN_WAIT_MS = 5_000;  // wait after feeding to let retries finish
    static final String TOPIC = "T";
    static final long STATS_INTERVAL_MS = 2_000;  // print stats every ~2s

    public static void main(String[] args) throws Exception {
        // ----- metrics -----
        ConcurrentHashMap<String, LongAdder> counters = new ConcurrentHashMap<>();
        IMetrics metrics = IMetrics.memoryMetrics(counters);

        // ----- time service -----
        ITimeService time = ITimeService.real;

        // ----- ladder -----
        List<WorkLeaseStage> stages = new ArrayList<>();
        for (int t : RETRY_TIMEOUTS_MS) {
            stages.add(new WorkLeaseStage("stage_" + t, t, JITTER_MS, null));
        }

        // ----- buckets + runner -----
        RetryBuckets buckets = new RetryBuckets(stages, time, GRANULARITY_MS, metrics);

        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "soak-runner-scheduler");
            t.setDaemon(true);
            return t;
        });

        ScheduledExecutorService statsScheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "soak-stats");
            t.setDaemon(true);
            return t;
        });

        ConcurrentHashMap<String, Integer> attempts = new ConcurrentHashMap<>();
        LongAdder successCount = new LongAdder();
        LongAdder producedCount = new LongAdder();
        Random rnd = new Random(42);

        BucketRunner runner = new BucketRunner(
                buckets,
                GRANULARITY_MS,
                scheduler,
                time,
                (k, delay) -> { /* optional: observe scheduling */ },
                metrics,
                key -> {
                    String id = key.topic() + "|" + key.domainId();
                    int currentAttempt = attempts.getOrDefault(id, 0);
                    boolean fail = (rnd.nextInt(FAILURE_ONE_IN) == 0);

                    if (fail) {
                        int nextAttempt = currentAttempt + 1;
                        attempts.put(id, nextAttempt);
                        boolean scheduled = buckets.addToRetryBucket(
                                key.topic(), key.domainId(), nextAttempt, time.currentTimeMillis(), nextAttempt
                        );
                        if (!scheduled) {
                            // out of ladder → give-up already counted in metrics
                            attempts.remove(id);
                        }
                    } else {
                        successCount.increment();
                        attempts.remove(id);
                    }
                    return CompletableFuture.completedFuture(null);
                }
        );
        runner.start();

        // ----- live stats printer -----
        long start = System.currentTimeMillis();
        ScheduledFuture<?> statsFuture = statsScheduler.scheduleAtFixedRate(() -> {
            long now = System.currentTimeMillis();
            long elapsed = (now - start) / 1000;
            long successes = successCount.sum();
            long giveUps = sum(counters, "retryBuckets.giveUp");
            long added = sum(counters, "retryBuckets.added");
            long removed = sum(counters, "retryBuckets.removed");
            long scheduled = sum(counters, "retryBucketRunner.scheduled");
            int inFlight = attempts.size();
            // queued = total keys currently in buckets (sum per bucket)
            int queued = buckets.buckets().values().stream()
                    .mapToInt(rb -> rb.keys().size())
                    .sum();

            System.out.printf(
                    "[%3ds] produced=%d successes=%d giveUps=%d inFlight=%d queued=%d | added=%d removed=%d scheduled=%d%n",
                    elapsed, producedCount.sum(), successes, giveUps, inFlight, queued, added, removed, scheduled
            );
        }, 0, STATS_INTERVAL_MS, TimeUnit.MILLISECONDS);

        // ----- generate events -----
        long totalEvents = (long) DURATION_SECONDS * EVENTS_PER_SECOND;
        for (int s = 0; s < DURATION_SECONDS; s++) {
            long batchStart = System.currentTimeMillis();
            for (int i = 0; i < EVENTS_PER_SECOND; i++) {
                String domain = "d-" + s + "-" + i;
                String id = TOPIC + "|" + domain;
                attempts.put(id, 0);
                producedCount.increment();
                buckets.addToRetryBucket(TOPIC, domain, i, time.currentTimeMillis(), 0);
            }
            long elapsed = System.currentTimeMillis() - batchStart;
            long sleep = 1_000L - elapsed;
            if (sleep > 0) Thread.sleep(sleep);
        }

        // ----- allow retries to drain -----
        Thread.sleep(POST_DRAIN_WAIT_MS);

        // shutdown
        runner.close();
        scheduler.shutdown();
        statsFuture.cancel(false);
        statsScheduler.shutdown();
        scheduler.awaitTermination(2, TimeUnit.SECONDS);
        statsScheduler.awaitTermination(2, TimeUnit.SECONDS);

        long giveUps = sum(counters, "retryBuckets.giveUp");
        long successes = successCount.sum();

        for (String key : counters.keySet()) {
            System.out.printf("%s: %d%n", key, counters.get(key).sum());
        }
        System.out.println();
        System.out.println("---- Soak Test Results ----");
        System.out.printf("Total events: %d%n", totalEvents);
        System.out.printf("Successes   : %d%n", successes);
        System.out.printf("GiveUps     : %d%n", giveUps);
        System.out.printf("Buckets added   : %d%n", sum(counters, "retryBuckets.added"));
        System.out.printf("Buckets removed : %d%n", sum(counters, "retryBuckets.removed"));
        System.out.printf("Runner started  : %d%n", sum(counters, "retryBucketRunner.started"));
        System.out.printf("Runner scheduled: %d%n", sum(counters, "retryBucketRunner.scheduled"));
        System.out.printf("Runner stopped  : %d%n", sum(counters, "retryBucketRunner.stopped"));
        System.out.println();
        for (int i = 0; i < RETRY_TIMEOUTS_MS.length; i++) {
            String stageName = "stage_" + RETRY_TIMEOUTS_MS[i];
            long added = sum(counters, "retryBuckets." + stageName + ".added");
            System.out.printf("Stage %s: added=%d%n", stageName, added);
        }

        if (successes + giveUps != totalEvents) {
            throw new AssertionError("Mismatch: successes + giveUps != total events");
        }
    }

    private static long sum(ConcurrentHashMap<String, LongAdder> counters, String name) {
        var a = counters.get(name);
        return a == null ? 0L : a.sum();
    }
}
