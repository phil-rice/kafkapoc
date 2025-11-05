package com.hcltech.rmg.flinkadapters;

import java.text.DecimalFormat;
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

    private static volatile long startWhenVPositive = -1; // NEW: track the start time when V > 0

    private static final AtomicBoolean STARTED = new AtomicBoolean(false);
    private static ScheduledExecutorService SCHED;

    // For formatting numbers with commas
    private static final DecimalFormat formatter = new DecimalFormat("#,###");

    public static void clear(){
        VALUES.reset();
        ERRORS.reset();
        RETRIES.reset();
        FAILURES.reset();
        startWhenVPositive = -1; // Reset the start time
    }

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

        // Record start time when V first becomes positive
        if (v > 0 && startWhenVPositive == -1) {
            startWhenVPositive = now; // Record when V > 0
        }

        // Time elapsed since the first value was recorded
        long elapsedSinceStart = (startWhenVPositive != -1) ? (now - startWhenVPositive) / 1000 : 0;

        double winRate  = (dv + de + dr + df) * 1000.0 / dt;
        double allRate  = (v + e + r + f)     * 1000.0 / dtOverall;
        double errPct   = (v + e) == 0 ? 0.0 : (e * 100.0 / (v + e));
        double failPct  = (v + f) == 0 ? 0.0 : (f * 100.0 / (v + f));

        // Formatting the numbers with commas
        String formattedV = format(v);
        String formattedE = format(e);
        String formattedR = format(r);
        String formattedF = format(f);

        String formattedDeltaV = format(dv);
        String formattedDeltaE = format(de);
        String formattedDeltaR = format(dr);
        String formattedDeltaF = format(df);

        // Format the rates and make sure they align without extra space
        String winRateFormatted = String.format("%,.0f", winRate);
        String allRateFormatted = String.format("%,.0f", allRate);

        // Calculate the width for the rate columns dynamically
        int rateWidth = Math.max(winRateFormatted.length(), allRateFormatted.length());

        // Print with aligned columns for values and deltas, but compact for errors, retries, and failures
        System.out.printf(
                "[perf] v=%-10s e=%-6s r=%-6s f=%-6s | Δv=%-10s Δe=%-6s Δr=%-6s Δf=%-6s over %.1fs | rate=%-" + rateWidth + "s/s (overall %-" + rateWidth + "s/s) | err=%.2f%% fail=%.2f%% | time=%d%n",
                formattedV, formattedE, formattedR, formattedF,
                formattedDeltaV, formattedDeltaE, formattedDeltaR, formattedDeltaF,
                dt / 1000.0,
                winRateFormatted, allRateFormatted,
                errPct, failPct,
                elapsedSinceStart // Show time since V > 0
        );

        lastV = v; lastE = e; lastR = r; lastF = f; lastTs = now;
    }

    // Helper method to format large numbers with commas
    private static String format(long value) {
        return formatter.format(value);
    }

    // Helper method to format rates and other decimal numbers
    private static String format(double value) {
        return String.format("%,.0f", value); // Format as a number with commas
    }
}
