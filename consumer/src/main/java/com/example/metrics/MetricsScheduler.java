package com.example.metrics;

/**
 * Background scheduler: ticks every periodMs, takes snapshot, computes delta + rate, prints.
 */
public final class MetricsScheduler<S> implements AutoCloseable, Runnable {
    private final MetricsRegistry<S> registry;
    private final MetricsPrinter<S> printer;
    private final long periodMs;

    private volatile boolean running = true;
    private Thread thread;

    // state for deltas
    private long lastTime = System.currentTimeMillis();
    private long lastTotal = 0L;

    public MetricsScheduler(MetricsRegistry<S> registry, MetricsPrinter<S> printer, long periodMs) {
        this.registry = registry;
        this.printer = printer;
        this.periodMs = periodMs;
    }

    /** Start the background scheduler thread. */
    public void start() {
        thread = new Thread(this, "metrics-scheduler");
        thread.setDaemon(true);
        thread.start();
    }

    @Override
    public void run() {
        while (running) {
            try {
                Thread.sleep(periodMs);
            } catch (InterruptedException ie) {
                if (!running) break;
            }
            long now = System.currentTimeMillis();
            MetricsSnapshot<S> snap = registry.snapshot();
            long delta = snap.totalProcessed() - lastTotal;
            double rate = (now > lastTime) ? (delta * 1000.0) / (now - lastTime) : 0.0;
            printer.print(snap, delta, rate);
            lastTime = now;
            lastTotal = snap.totalProcessed();
        }
    }

    @Override
    public void close() {
        running = false;
        if (thread != null) {
            thread.interrupt();
            try {
                thread.join(5000);
            } catch (InterruptedException ignored) {
                Thread.currentThread().interrupt();
            }
        }
    }
}
