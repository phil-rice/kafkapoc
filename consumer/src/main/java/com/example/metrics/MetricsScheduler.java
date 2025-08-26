package com.example.metrics;

public final class MetricsScheduler<S> implements AutoCloseable, Runnable {
    private final MetricsRegistry<S> registry;
    private final MetricsPrinter<S> printer;
    private final long periodMs;
    private final String primaryCounterName; // e.g., "processed"

    private volatile boolean running = true;
    private Thread thread;

    private long lastTime = System.currentTimeMillis();
    private long lastTotal = 0L;

    public MetricsScheduler(MetricsRegistry<S> registry,
                            MetricsPrinter<S> printer,
                            long periodMs,
                            String primaryCounterName) {
        this.registry = registry;
        this.printer = printer;
        this.periodMs = periodMs;
        this.primaryCounterName = primaryCounterName;
    }

    /** Back-compat convenience ctor defaults to "processed". */
    public MetricsScheduler(MetricsRegistry<S> registry,
                            MetricsPrinter<S> printer,
                            long periodMs) {
        this(registry, printer, periodMs, "processed");
    }

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
            long total = snap.counterTotals().getOrDefault(primaryCounterName, 0L);
            long delta = total - lastTotal;
            double rate = (now > lastTime) ? (delta * 1000.0) / (now - lastTime) : 0.0;

            printer.print(snap, delta, rate, primaryCounterName);

            lastTime = now;
            lastTotal = total;
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
