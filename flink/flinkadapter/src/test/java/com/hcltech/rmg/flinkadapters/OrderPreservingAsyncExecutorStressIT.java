package com.hcltech.rmg.flinkadapters;

import com.hcltech.rmg.common.ITimeService;
import com.hcltech.rmg.common.async.*;
import com.hcltech.rmg.messages.Envelope;
import com.hcltech.rmg.messages.EnvelopeFailureAdapter;
import com.hcltech.rmg.messages.ValueEnvelope;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Stress test for the REAL operator:
 *  - Async userFn (1..10ms jitter) on a real pool
 *  - 100ms processing-time drain timer exercised
 *  - Verifies completion, per-key order, multi-threading and that setKey() runs on operator thread.
 */
public class OrderPreservingAsyncExecutorStressIT {

    /** Minimal concrete Envelope for tests. */
    public static final class TestEnv implements Envelope<Object, Object> {
        private final String domainId;
        private final String corrId;

        public TestEnv(String domainId, String corrId) {
            this.domainId = domainId;
            this.corrId = corrId;
        }

        @Override public ValueEnvelope<Object, Object> valueEnvelope() {
            // Provide a non-null ValueEnvelope; internals are not used by the operator under test
            return new ValueEnvelope<>(null, null, null, new ArrayList<>());
        }

        @Override public String domainId() { return domainId; }
        public  String correlationId()     { return corrId;   }
    }

    /**
     * Concrete operator subclass so we can inject a userFn and capture setKey() thread names.
     * We wire the executor exactly like the real operator, but we do NOT call super.open()
     * to avoid DI. The harness provides runtime services (processing time, etc).
     */
    public static final class TestEnvelopeOp
            extends AbstractEnvelopeAsyncProcessingFunction<Object, Object, Object, Object, Object, Object> {

        private final OrderPreservingAsyncExecutor.UserFnPort<
                Envelope<Object, Object>, Envelope<Object, Object>,
                Output<StreamRecord<Envelope<Object, Object>>>> injectedUserFn;

        private final List<String> setKeyThreads;
        private final int laneCount, laneDepth, maxInFlight, executorThreads;
        private final long timeoutMs;

        // Internal infra we wire in open()
        private ILanes<Envelope<Object, Object>> lanesLocal;
        private IMpscRing<Output<StreamRecord<Envelope<Object, Object>>>, Envelope<Object, Object>, Envelope<Object, Object>> ringLocal;
        private PermitManager permitsLocal;
        private ExecutorService ioPoolLocal;

        public TestEnvelopeOp(
                OrderPreservingAsyncExecutor.UserFnPort<
                        Envelope<Object, Object>, Envelope<Object, Object>,
                        Output<StreamRecord<Envelope<Object, Object>>>> userFn,
                List<String> setKeyThreads,
                int laneCount, int laneDepth, int maxInFlight, int executorThreads, long timeoutMs
        ) {
            super(null); // bypass DI for this test; we wire in open()
            this.injectedUserFn  = userFn;
            this.setKeyThreads   = setKeyThreads;
            this.laneCount       = laneCount;
            this.laneDepth       = laneDepth;
            this.maxInFlight     = maxInFlight;
            this.executorThreads = executorThreads;
            this.timeoutMs       = timeoutMs;
        }

        // Required by the abstract base but unused in this test
        @Override
        protected OrderPreservingAsyncExecutor.UserFnPort<
                Envelope<Object, Object>, Envelope<Object, Object>,
                Output<StreamRecord<Envelope<Object, Object>>>> createUserFnPort(
                com.hcltech.rmg.appcontainer.interfaces.AppContainer<
                        Object, Object, Object, Object, Object,
                        Output<StreamRecord<Envelope<Object, Object>>>, Object> appContainer) {
            return injectedUserFn;
        }

        @Override
        protected void protectedSetupInOpen(
                com.hcltech.rmg.appcontainer.interfaces.AppContainer<
                        Object, Object, Object, Object, Object,
                        Output<StreamRecord<Envelope<Object, Object>>>, Object> appContainer) {
            // no-op
        }

        @Override
        public void open() throws Exception {
            // DO NOT call super.open(); that would try to resolve DI via appContainerDefn (null here).

            // FR typeclass that emits to operator output
            this.frType = new FlinkOutputFutureRecordAdapter<>(new EnvelopeFailureAdapter<>("stress-op"));

            // Route by domainId prefix before '|' so K-<key>|<seq> aligns to one lane per key
            Correlator<Envelope<Object, Object>> corr = new Correlator<>() {
                @Override public String correlationId(Envelope<Object, Object> env) { return ((TestEnv) env).correlationId(); }
                @Override public int laneHash(Envelope<Object, Object> env) {
                    String did = env.domainId();
                    int bar = did.indexOf('|');
                    String keyPrefix = (bar >= 0) ? did.substring(0, bar) : did; // "K-<key>"
                    return keyPrefix.hashCode();
                }
            };

            lanesLocal   = new Lanes<>(laneCount, laneDepth, corr);
            ringLocal    = new MpscRing<>(Math.max(1024, maxInFlight * 2));
            permitsLocal = new AtomicPermitManager(maxInFlight);
            ioPoolLocal  = Executors.newFixedThreadPool(Math.max(4, executorThreads));

            OrderPreservingAsyncExecutorConfig<
                    Envelope<Object, Object>,
                    Envelope<Object, Object>,
                    Output<StreamRecord<Envelope<Object, Object>>>> localCfg =
                    new OrderPreservingAsyncExecutorConfig<>(
                            laneCount, laneDepth, maxInFlight,
                            executorThreads, timeoutMs,
                            corr,
                            new FailureAdapter<>() {
                                @Override public Envelope<Object, Object> onFailure(Envelope<Object, Object> env, Throwable error) { return env; }
                                @Override public Envelope<Object, Object> onTimeout(Envelope<Object, Object> env, long elapsedNanos) { return env; }
                            },
                            frType,
                            ITimeService.real
                    );

            this.userFn = injectedUserFn;
            this.exec   = new OrderPreservingAsyncExecutor<>(
                    localCfg, lanesLocal, permitsLocal, ringLocal, ioPoolLocal, frType, laneCount, userFn
            );

            // schedule periodic drain on operator thread (100ms tick)
            var pts = getProcessingTimeService();
            long now = pts.getCurrentProcessingTime();
            pts.registerTimer(now + 100L, this);
        }

        @Override
        protected void setKey(Envelope<Object, Object> in, Envelope<Object, Object> out) {
            setKeyThreads.add(Thread.currentThread().getName()); // capture operator thread name
            this.setCurrentKey(in.domainId());
        }

        /** Test-only helper to trigger a drain using the CURRENT operator output (no FR caching). */
        public void drainTickForTest() {
            exec.drain(output, this::setKey);
        }

        @Override
        public void close() throws Exception {
            try {
                if (exec != null) exec.close();
            } finally {
                if (ioPoolLocal != null) ioPoolLocal.shutdownNow();
                super.close();
            }
        }
    }

    @Timeout(60)
    @Test
    void stress_real_operator_variable_delay_and_timer() throws Exception {
        final int TOTAL = 6000;
        final int KEYS  = 400;

        // capture pool worker threads + setKey threads
        Set<String> workerThreads  = ConcurrentHashMap.newKeySet();
        List<String> setKeyThreads = Collections.synchronizedList(new ArrayList<>());

        // Async userFn: 1..10ms jitter, identity transform (returns the same envelope)
        OrderPreservingAsyncExecutor.UserFnPort<
                Envelope<Object, Object>, Envelope<Object, Object>,
                Output<StreamRecord<Envelope<Object, Object>>>> userFn =
                (tc, in, corr, c) -> {
                    workerThreads.add(Thread.currentThread().getName());
                    try { Thread.sleep(1 + ThreadLocalRandom.current().nextInt(10)); } catch (InterruptedException ignored) {}
                    c.success(in, corr, in);
                };


        // Disable admission-time eviction for this test by setting timeoutMs = 0
                TestEnvelopeOp op = new TestEnvelopeOp(
                                userFn, setKeyThreads,
                                /*lanes*/256, /*depth*/8, /*maxInFlight*/256, /*pool threads*/8, /*timeoutMs*/0
                        );
//        );

        // Flink 2.0 harness: single-arg ctor
        @SuppressWarnings("unchecked")
        OneInputStreamOperatorTestHarness<Envelope<Object, Object>, Envelope<Object, Object>> h =
                new OneInputStreamOperatorTestHarness<>((OneInputStreamOperator<Envelope<Object, Object>, Envelope<Object, Object>>) op);

        h.open();

        // Feed inputs: domainId = "K-<key>|<seq>", corrId = "C-<i>"
        AtomicInteger[] keySeq = new AtomicInteger[KEYS];
        for (int i = 0; i < KEYS; i++) keySeq[i] = new AtomicInteger();

        for (int i = 0; i < TOTAL; i++) {
            int k = i % KEYS;
            int s = keySeq[k].getAndIncrement();
            String domainId = "K-" + k + "|" + s;
            String corrId   = "C-" + i;

            h.processElement(new StreamRecord<>(new TestEnv(domainId, corrId)));

            // periodically advance processing time to fire the 100ms drain timer
            if ((i & 31) == 31) {
                h.setProcessingTime(h.getProcessingTime() + 100L);
            }
        }

        // Post-feed: keep advancing time AND trigger explicit drainer on operator thread until completion
        final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(60);
        boolean diagPrinted = false;

        while (true) {
            // explicit drain using the operator’s CURRENT output — no FR caching
            op.drainTickForTest();

            // also tick the timer
            h.setProcessingTime(h.getProcessingTime() + 100L);

            List<Envelope<Object, Object>> out = h.extractOutputValues();
            if (out.size() >= TOTAL) break;

            long now = System.nanoTime();
            if (!diagPrinted && now > deadline - TimeUnit.SECONDS.toNanos(5)) {
                System.out.printf("[DIAG] emitted %,d / %,d…%n", out.size(), TOTAL);
                diagPrinted = true;
            }
            if (now >= deadline) {
                fail("Timed out waiting for completions; emitted " + out.size() + " of " + TOTAL);
            }
            Thread.sleep(2);
        }

        // Gather final output
        List<Envelope<Object, Object>> out = h.extractOutputValues();

        // 1) All items completed
        assertEquals(TOTAL, out.size(), "all items should complete");

        // 2) Per-key order preserved: increasing <seq> for each K-<key>
        Map<String,Integer> last = new HashMap<>();
        for (Envelope<Object, Object> env : out) {
            String id  = env.domainId();               // "K-<key>|<seq>"
            int bar    = id.indexOf('|');
            String key = (bar > 0) ? id.substring(0, bar) : id; // "K-<key>"
            int seq    = (bar > 0) ? Integer.parseInt(id.substring(bar + 1)) : 0;

            Integer prev = last.put(key, seq);
            if (prev != null && seq <= prev) {
                fail("Order violated for key " + key + ": " + prev + " -> " + seq);
            }
        }

        // 3) Evidence of multi-threaded execution
        assertTrue(workerThreads.size() >= 2, "work should span multiple pool threads: " + workerThreads);

        // 4) setKey only on operator (harness) thread — all names identical
        assertFalse(setKeyThreads.isEmpty(), "setKey should have been called");
        String opThread = setKeyThreads.get(0);
        for (String t : setKeyThreads) {
            assertEquals(opThread, t, "setKey must run only on the operator thread");
        }

        h.close();
    }
}
