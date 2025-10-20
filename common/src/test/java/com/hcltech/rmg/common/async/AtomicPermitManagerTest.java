package com.hcltech.rmg.common.async;

import static org.junit.jupiter.api.Assertions.*;

import java.time.Duration;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Test;

class AtomicPermitManagerTest {

    @Test
    void initialState_isMaxAvailable() {
        PermitManager pm = new AtomicPermitManager(256);
        assertEquals(256, pm.maxPermits());
        assertEquals(256, pm.availablePermits());
    }

    @Test
    void acquireUpToLimit_thenReject_thenReleaseAllowsAgain() {
        int max = 8;
        PermitManager pm = new AtomicPermitManager(max);

        for (int i = 0; i < max; i++) {
            assertTrue(pm.tryAcquire(), "permit " + i + " should be acquirable");
        }
        assertEquals(0, pm.availablePermits());

        // Non-blocking rejection when exhausted
        assertFalse(pm.tryAcquire());
        assertEquals(0, pm.availablePermits());

        // Release enables a new launch
        pm.release();
        assertEquals(1, pm.availablePermits());
        assertTrue(pm.tryAcquire());
        assertEquals(0, pm.availablePermits());
    }

    @Test
    void tryAcquire_isNonBlockingWhenExhausted() {
        PermitManager pm = new AtomicPermitManager(2);
        assertTrue(pm.tryAcquire());
        assertTrue(pm.tryAcquire());
        assertEquals(0, pm.availablePermits());

        // Should return immediately (no blocking/parking)
        assertTimeoutPreemptively(Duration.ofMillis(20), () -> {
            assertFalse(pm.tryAcquire());
        });
    }

    @Test
    void concurrentAcquisition_neverExceedsMaxInFlight() throws Exception {
        final int max = 5;           // small for fast, deterministic test
        final int threads = 16;
        final int roundsPerThread = 20;

        PermitManager pm = new AtomicPermitManager(max);
        ExecutorService pool = Executors.newFixedThreadPool(threads);

        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch done  = new CountDownLatch(threads);

        AtomicInteger current = new AtomicInteger(0);
        AtomicInteger peak    = new AtomicInteger(0);
        AtomicInteger launches= new AtomicInteger(0);

        Runnable r = () -> {
            try {
                start.await();
                for (int i = 0; i < roundsPerThread; i++) {
                    if (pm.tryAcquire()) {
                        int now = current.incrementAndGet();
                        peak.updateAndGet(p -> Math.max(p, now));
                        launches.incrementAndGet();
                        try { Thread.sleep(1); } catch (InterruptedException ignored) {}
                        current.decrementAndGet();
                        pm.release();
                    } else {
                        try { Thread.sleep(1); } catch (InterruptedException ignored) {}
                    }
                }
            } catch (InterruptedException ignored) {
                Thread.currentThread().interrupt();
            } finally {
                done.countDown();
            }
        };

        for (int i = 0; i < threads; i++) pool.submit(r);
        start.countDown();
        assertTrue(done.await(5, TimeUnit.SECONDS), "workers timed out");
        pool.shutdownNow();

        assertTrue(peak.get() <= max, "peak in-flight " + peak.get() + " > " + max);
        assertEquals(pm.maxPermits(), pm.availablePermits(), "leaked permits");
        assertTrue(launches.get() > 0, "no launches observed");
    }

    @Test
    void releaseBeyondMax_throws() {
        PermitManager pm = new AtomicPermitManager(3);

        // Acquire all, then release all: OK
        assertTrue(pm.tryAcquire());
        assertTrue(pm.tryAcquire());
        assertTrue(pm.tryAcquire());
        pm.release();
        pm.release();
        pm.release();
        assertEquals(3, pm.availablePermits());

        // Over-release must throw (catches wiring bugs early)
        assertThrows(IllegalStateException.class, pm::release);
        assertEquals(3, pm.availablePermits(), "state should remain at max after failed over-release");
    }

    @Test
    void constructorRejectsNonPositiveMax() {
        assertThrows(IllegalArgumentException.class, () -> new AtomicPermitManager(0));
        assertThrows(IllegalArgumentException.class, () -> new AtomicPermitManager(-1));
    }
}
