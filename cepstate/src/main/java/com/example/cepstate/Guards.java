package com.example.cepstate;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

public final class Guards {
    private Guards() {}

    /**
     * Wraps a runnable so it will never re-enter if still running from a previous call.
     * If the body throws, the guard is released and the exception is rethrown.
     */
    public static Runnable noReentry(Runnable body) {
        AtomicBoolean running = new AtomicBoolean(false);
        return () -> {
            if (!running.compareAndSet(false, true)) {
                return; // skip overlapping call
            }
            try {
                body.run();
            } finally {
                running.set(false);
            }
        };
    }

    /**
     * Like {@link #noReentry(Runnable)} but allows returning a value.
     * If the body is still running, returns {@code null}.
     */
    public static <T> Supplier<T> noReentry(Supplier<T> body) {
        AtomicBoolean running = new AtomicBoolean(false);
        return () -> {
            if (!running.compareAndSet(false, true)) {
                return null;
            }
            try {
                return body.get();
            } finally {
                running.set(false);
            }
        };
    }
}
