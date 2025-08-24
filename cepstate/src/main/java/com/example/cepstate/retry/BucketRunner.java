package com.example.cepstate.retry;

import com.example.cepstate.metrics.IMetrics;
import com.example.kafka.common.ITimeService;

import java.util.List;
import java.util.concurrent.*;
import java.util.function.BiConsumer;
import java.util.function.Function;

public final class BucketRunner implements AutoCloseable {
    private final IRetryBuckets buckets;
    private final long tickMillis; // usually = your bucket granularity
    private final ScheduledExecutorService scheduler; // 1 thread for timing
    private final Function<RetryKey, CompletableFuture<?>> handler;
    private final ITimeService timeService;
    private final BiConsumer<RetryKey, Long> callback;
    private final IMetrics metrics;
    private ScheduledFuture<?> tickFuture;

    public BucketRunner(IRetryBuckets buckets, long tickMillis,
                 ScheduledExecutorService scheduler,
                 ITimeService timeService,
                 BiConsumer<RetryKey, Long> callback,
                 IMetrics metrics,
                 Function<RetryKey, CompletableFuture<?>> handler) {
        this.timeService = timeService;
        this.callback = callback;
        this.metrics = metrics;
        if (tickMillis <= 0) throw new IllegalArgumentException("tickMillis must be > 0");
        this.buckets = buckets;
        this.tickMillis = tickMillis;
        this.scheduler = scheduler;
        this.handler = handler;
    }

    public void start() {
        //note that if the previous drainAndSchedule is still running, this will not wait for it to finish.
        // It's hard to know what the correct behavior is in that case. So for now we just run it again in parallel
        tickFuture = scheduler.scheduleAtFixedRate(
                this::drainAndSchedule,
                0, tickMillis, TimeUnit.MILLISECONDS);
        metrics.increment("retryBucketRunner.started");
    }

    private void drainAndSchedule() {
        long now = timeService.currentTimeMillis();
        List<RetryKey> dueOrNear = buckets.retryKeysForNow();  // returns items whose bucket is due
        for (RetryKey k : dueOrNear) {
            long delay = Math.max(0L, k.dueAtMs() - now); // honor per-item jitter
            callback.accept(k, delay);
            metrics.increment("retryBucketRunner.scheduled");
            scheduler.schedule(() -> {
                try {
                    handler.apply(k);
                } catch (Throwable t) {
                    t.printStackTrace(); //It was the job of the handler to handle exceptions. However we at least should log it.
                }
            }, delay, TimeUnit.MILLISECONDS);
        }
    }

    @Override
    public void close() {
        if (tickFuture != null) {
            metrics.increment("retryBucketRunner.stopped");
            tickFuture.cancel(false);
        }
        // Caller owns scheduler lifecycle: scheduler.shutdown() outside if desired.
    }

    // Convenience factory for typical use (daemon scheduler)
    static BucketRunner startDefault(IRetryBuckets buckets, long tickMillis,
                                     ITimeService timeService,
                                     BiConsumer<RetryKey, Long> callback,
                                     IMetrics metrics,
                                     Function<RetryKey, CompletableFuture<?>> handler) {
        ScheduledExecutorService sch = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "retry-bucket-scheduler");
            t.setDaemon(true);
            return t;
        });
        BucketRunner r = new BucketRunner(buckets, tickMillis, sch, timeService, callback,metrics, handler);
        r.start();
        return r;
    }
}
