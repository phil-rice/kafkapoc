package com.hcltech.rmg.common.async;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.HashSet;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.*;

final class MpscRingTest {

    static final class Rec {
        final String kind; // "ok" or "err"
        final String in;
        final long corr;
        final String out;       // when ok
        final String errorType; // when err

        Rec(String kind, String in, long corr, String out, String errorType) {
            this.kind = kind; this.in = in; this.corr = corr; this.out = out; this.errorType = errorType;
        }

        @Override public String toString() {
            return kind + "(" + in + "," + corr + "," + (out != null ? out : errorType) + ")";
        }
    }

    // --------------------- construction / validation ---------------------

    @Test
    void ctor_validates_powerOfTwo_and_positive() {
        assertThrows(IllegalArgumentException.class, () -> new MpscRing<String,String>(0));
        assertThrows(IllegalArgumentException.class, () -> new MpscRing<String,String>(3));
        assertDoesNotThrow(() -> new MpscRing<String,String>(1));
        assertDoesNotThrow(() -> new MpscRing<String,String>(8));
    }

    // --------------------- single producer / single consumer ---------------------

    @Test
    void singleProducer_singleConsumer_preservesOrder_andPayloads() {
        IMpscRing<String,String> ring = new MpscRing<>(8);
        List<Rec> got = new ArrayList<>();

        // produce 5 ok and 2 err
        ring.offerSuccess("a", 1, "A");
        ring.offerSuccess("b", 2, "B");
        ring.offerFailure("c", 3, new IllegalStateException("boom"));
        ring.offerSuccess("d", 4, "D");
        ring.offerFailure("e", 5, new RuntimeException("oops"));
        ring.offerSuccess("f", 6, "F");
        ring.offerSuccess("g", 7, "G");

        while (true) {
            int drained = ring.drain(new IMpscRing.Handler<>() {
                @Override public void onSuccess(String in, long corr, String out) {
                    got.add(new Rec("ok", in, corr, out, null));
                }
                @Override public void onFailure(String in, long corr, Throwable err) {
                    got.add(new Rec("err", in, corr, null, err.getClass().getSimpleName()));
                }
            });
            if (drained == 0) break;
        }

        assertEquals(7, got.size());
        assertEquals("ok",  got.get(0).kind); assertEquals("a", got.get(0).in); assertEquals(1, got.get(0).corr); assertEquals("A", got.get(0).out);
        assertEquals("ok",  got.get(1).kind); assertEquals("b", got.get(1).in); assertEquals(2, got.get(1).corr); assertEquals("B", got.get(1).out);
        assertEquals("err", got.get(2).kind); assertEquals("c", got.get(2).in); assertEquals(3, got.get(2).corr); assertEquals("IllegalStateException", got.get(2).errorType);
        assertEquals("ok",  got.get(3).kind); assertEquals("d", got.get(3).in); assertEquals(4, got.get(3).corr); assertEquals("D", got.get(3).out);
        assertEquals("err", got.get(4).kind); assertEquals("e", got.get(4).in); assertEquals(5, got.get(4).corr); assertEquals("RuntimeException", got.get(4).errorType);
        assertEquals("ok",  got.get(5).kind); assertEquals("f", got.get(5).in); assertEquals(6, got.get(5).corr); assertEquals("F", got.get(5).out);
        assertEquals("ok",  got.get(6).kind); assertEquals("g", got.get(6).in); assertEquals(7, got.get(6).corr); assertEquals("G", got.get(6).out);

        // further drain should be empty
        assertNull(ring.pollOne());
    }

    // --------------------- multi producer / single consumer ---------------------

    @Test
    void manyProducers_singleConsumer_allDelivered_noDuplicates() throws Exception {
        final int capacity = 1024;
        final int producers = 8;
        final int perProducer = 5000; // total 40k
        final int total = producers * perProducer;

        IMpscRing<String,String> ring = new MpscRing<>(capacity);

        ExecutorService pool = Executors.newFixedThreadPool(producers);
        AtomicLong corrGen = new AtomicLong(1);
        CountDownLatch started = new CountDownLatch(producers);
        CountDownLatch done = new CountDownLatch(producers);

        Runnable producer = () -> {
            started.countDown();
            try {
                started.await();
            } catch (InterruptedException e) { Thread.currentThread().interrupt(); }

            for (int i = 0; i < perProducer; i++) {
                long corr = corrGen.getAndIncrement();
                // Busy-wait if temporarily full relative to this ticket: keep retrying
                for (;;) {
                    if (ring.offerSuccess("in-"+corr, corr, "out-"+corr)) break;
                    Thread.onSpinWait();
                }
            }
            done.countDown();
        };
        for (int i = 0; i < producers; i++) pool.submit(producer);

        // single consumer loop
        List<Rec> got = Collections.synchronizedList(new ArrayList<>(total));
        while (done.getCount() > 0 || got.size() < total) {
            ring.drain(new IMpscRing.Handler<>() {
                @Override public void onSuccess(String in, long corr, String out) {
                    got.add(new Rec("ok", in, corr, out, null));
                }
                @Override public void onFailure(String in, long corr, Throwable err) {
                    got.add(new Rec("err", in, corr, null, err.getClass().getSimpleName()));
                }
            });
        }

        pool.shutdownNow();

        assertEquals(total, got.size(), "missing records");

        // no duplicates, all corrs present
        Set<Long> corrs = new HashSet<>(total);
        for (Rec r : got) {
            assertEquals("ok", r.kind);
            assertTrue(corrs.add(r.corr), "duplicate corrId " + r.corr);
            assertEquals("in-"+r.corr, r.in);
            assertEquals("out-"+r.corr, r.out);
        }
    }

    // --------------------- failure path ---------------------

    @Test
    void failure_is_delivered_with_throwable() {
        IMpscRing<String,String> ring = new MpscRing<>(16);
        List<Rec> got = new ArrayList<>();

        ring.offerFailure("X", 42, new TimeoutException("late"));

        ring.drain(new IMpscRing.Handler<>() {
            @Override public void onSuccess(String in, long corr, String out) {
                fail("expected failure");
            }
            @Override public void onFailure(String in, long corr, Throwable err) {
                got.add(new Rec("err", in, corr, null, err.getClass().getSimpleName()));
            }
        });

        assertEquals(1, got.size());
        assertEquals("err", got.get(0).kind);
        assertEquals("X", got.get(0).in);
        assertEquals(42, got.get(0).corr);
        assertEquals("TimeoutException", got.get(0).errorType);
    }

    // --------------------- wrap-around under interleaving ---------------------

    @Test
    void wrapAround_smallCapacity_interleaved_produce_consume() {
        IMpscRing<String,String> ring = new MpscRing<>(4);
        List<Rec> got = new ArrayList<>();

        // produce 4, drain a few times, produce 4 more, drain all
        for (int i = 1; i <= 4; i++) ring.offerSuccess("in-"+i, i, "out-"+i);

        // drain two (in two calls)
        for (int k = 0; k < 2; k++) {
            ring.drain(new IMpscRing.Handler<>() {
                @Override public void onSuccess(String in, long corr, String out) {
                    got.add(new Rec("ok", in, corr, out, null));
                }
                @Override public void onFailure(String in, long corr, Throwable err) {
                    got.add(new Rec("err", in, corr, null, err.getClass().getSimpleName()));
                }
            });
        }

        for (int i = 5; i <= 8; i++) ring.offerSuccess("in-"+i, i, "out-"+i);

        // drain rest
        while (true) {
            int drained = ring.drain(new IMpscRing.Handler<>() {
                @Override public void onSuccess(String in, long corr, String out) {
                    got.add(new Rec("ok", in, corr, out, null));
                }
                @Override public void onFailure(String in, long corr, Throwable err) {
                    got.add(new Rec("err", in, corr, null, err.getClass().getSimpleName()));
                }
            });
            if (drained == 0) break;
        }

        assertEquals(8, got.size());
        // spot check start/end
        assertEquals(1, got.get(0).corr);
        assertEquals(8, got.get(got.size()-1).corr);
    }

    // --------------------- empty pollOne returns null ---------------------

    @Test
    void pollOne_returnsNull_when_empty() {
        IMpscRing<String,String> ring = new MpscRing<>(8);
        assertNull(ring.pollOne());
    }

    // helper functional interfaces to avoid noisy lambdas in assertions
    interface TimeoutExceptionProvider {
        TimeoutException get();
    }
    static final class TimeoutException extends RuntimeException {
        TimeoutException(String m) { super(m); }
    }
}
