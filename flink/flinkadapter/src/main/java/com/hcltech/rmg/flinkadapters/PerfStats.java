package com.hcltech.rmg.flinkadapters;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.LongAdder;

public final class PerfStats {
    private static final LongAdder VALUES   = new LongAdder();
    private static final LongAdder ERRORS   = new LongAdder();
    private static final LongAdder RETRIES  = new LongAdder();
    private static final LongAdder FAILURES = new LongAdder();   // NEW

    private static volatile long lastTs = System.currentTimeMillis();
    private static volatile long lastV  = 0, lastE = 0, lastR = 0, lastF = 0; // include lastF
    private static final long START_TS  = System.currentTimeMillis();

    private static final AtomicBoolean STARTED = new AtomicBoolean(false);
    private static ScheduledExecutorService SCHED;

    private PerfStats() {}

    // increments called from sinks
    static void incValues()   { VALUES.add(1); }
    static void incErrors()   { ERRORS.add(1); }
    static void incRetries()  { RETRIES.add(1); }
    static void incFailures() { FAILURES.add(1); } // NEW

    // expose totals for gauges
    static long values()   { return VALUES.sum(); }
    static long errors()   { return ERRORS.sum(); }
    static long retries()  { return RETRIES.sum(); }
    static long failures() { return FAILURES.sum(); } // NEW

    public static void start(long intervalMs) {
        if (!STARTED.compareAndSet(false, true)) return;
        long period = Math.max(500, intervalMs);
        SCHED = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "perf-stats-printer");
            t.setDaemon(true);
            return t;
        });
        SCHED.scheduleAtFixedRate(PerfStats::print, period, period, TimeUnit.MILLISECONDS);
    }

    private static void print() {
        long now = System.currentTimeMillis();
        long v = VALUES.sum(), e = ERRORS.sum(), r = RETRIES.sum(), f = FAILURES.sum();

        long dv = v - lastV, de = e - lastE, dr = r - lastR, df = f - lastF;
        long dt = Math.max(1, now - lastTs), dtOverall = Math.max(1, now - START_TS);

        double winRate  = (dv + de + dr + df) * 1000.0 / dt;
        double allRate  = (v + e + r + f)     * 1000.0 / dtOverall;
        double errPct   = (v + e) == 0 ? 0.0 : (e * 100.0 / (v + e));
        double failPct  = (v + f) == 0 ? 0.0 : (f * 100.0 / (v + f));

        System.out.printf(
                "[perf] v=%d e=%d r=%d f=%d | Δv=%d Δe=%d Δr=%d Δf=%d over %.1fs | rate=%.0f/s (overall %.0f/s) | err=%.2f%% fail=%.2f%%%n",
                v, e, r, f,
                dv, de, dr, df,
                dt / 1000.0,
                winRate, allRate,
                errPct, failPct
        );

        lastV = v; lastE = e; lastR = r; lastF = f; lastTs = now;
    }
}
