//package com.example.cepstate.int_tests;
//
//import com.example.cepstate.CepState;
//import com.example.kafka.common.ITimeService;
//import com.example.cepstate.metrics.IMetrics;
//import com.example.cepstate.retry.BucketRunner;
//import com.example.cepstate.retry.IRetryBuckets;
//import com.example.cepstate.retry.RetryBuckets;
//import com.example.cepstate.rocks.RocksCepState;
//import com.example.cepstate.worklease.*;
//import com.example.kafka.common.Codec;
//import com.example.optics.IOpticsEvent;
//import com.example.optics.Interpreter;
//import org.apache.commons.jxpath.JXPathContext;
//import org.rocksdb.Options;
//import org.rocksdb.RocksDB;
//
//import java.util.List;
//import java.util.Random;
//import java.util.concurrent.*;
//import java.util.concurrent.atomic.AtomicInteger;
//import java.util.concurrent.atomic.AtomicReference;
//import java.util.concurrent.atomic.LongAdder;
//import java.util.function.Function;
//import java.util.stream.Stream;
//
///**
// * Soak test for full pipeline using REAL RetryBuckets + BucketRunner + MemoryWorkLease + ProcessMessage.
// * <p>
// * It drives a large, random stream of domain/offsets:
// * - initial items are scheduled through the real buckets (stage 0)
// * - retries and backlog hand-offs are also scheduled by ProcessMessage via buckets
// * - a thin buckets decorator stashes offsets in a per-(topic,domain) mailbox so the runner can pop them
// * <p>
// * Success criteria:
// * - successes + giveUps == produced (unique offsets submitted)
// * - NO IllegalStateException surfaced (fencing/backlog invariant holds)
// */
//public class ProcessMessageBucketsSoakMain {
//
//    // ---------------- tunables ----------------
//    static final int NUM_DOMAINS = 100;
//    static final int DURATION_SECONDS = 5;
//    static final int EVENTS_PER_SECOND = 50_000;       // adjust up/down
//    static final int PROCESS_SYNC_FAIL_ONE_IN = 10;    // processor throws sync 1/N
//    static final int MUTATE_ASYNC_FAIL_ONE_IN = 25;    // mutate async fails 1/N (0 disables)
//    static final int MAX_RETRIES = 3;
//
//    static final long GRANULARITY_MS = 50L;            // bucket width/tick
//    static final int[] STAGE_DELAYS_MS = {0, 100, 200, 400};
//    static final int JITTER_MS = 25;
//
//    static final long seed = 12345L;
//    static final String TOPIC = "pm-soak-topic";
//    static final Stream<TestDomainMessage> stream = DomainMessageGenerator.randomUniformStream(NUM_DOMAINS, seed);
//
//    public static void main(String[] args) throws Exception {
//
//        // ----- time + rng -----
//        ITimeService time = System::currentTimeMillis;
//
//
//        // ----- metrics -----
//        ConcurrentHashMap<String, LongAdder> counters = new ConcurrentHashMap<>();
//        IMetrics metrics = IMetrics.memoryMetrics(counters);
//
//        // ----- executors -----
//        int submitThreads = Math.max(2, Runtime.getRuntime().availableProcessors());
//        ExecutorService submitPool = Executors.newFixedThreadPool(submitThreads, r -> {
//            Thread t = new Thread(r, "pm-soak-submit");
//            t.setDaemon(true);
//            return t;
//        });
//        ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(4, r -> {
//            Thread t = new Thread(r, "pm-soak-scheduler");
//            t.setDaemon(true);
//            return t;
//        });
//
//        // ----- WorkLease (counted) -----
//        MemoryWorkLease lease = new MemoryWorkLease(new DeterministicTokenGenerator(), time, MAX_RETRIES);
//        Options options = new Options().setCreateIfMissing(true);
//        RocksDB db = RocksDB.open(options, "./rocksdb");
//
//
//        // ----- CepState  -----
//        Codec<List<IOpticsEvent<JXPathContext>>, byte[]> codec = Codec.bytes(Codec.lines(IOpticsEvent.codec()));
//        CepState cepState = new RocksCepState<JXPathContext, Object>(db, codec, Interpreter.jxPathInterpreter());
//
//        // ----- stages -----
//        var stages = new java.util.ArrayList<WorkLeaseStage>();
//        for (int d : STAGE_DELAYS_MS) stages.add(new WorkLeaseStage("stage_" + d, d, JITTER_MS, null));
//
//        // ----- real buckets + runner -----
//        RetryBuckets realBuckets = new RetryBuckets(stages, time, GRANULARITY_MS, metrics);
//
//
//        BucketRunner runner = new BucketRunner(
//                realBuckets,
//                GRANULARITY_MS,
//                scheduler,
//                time,
//                (key, delay) -> { /* observe schedule if desired */ },
//                metrics,
//                key -> {
//                    // Pop the head offset for this (topic,domain); if none, it was likely drained already
//                    Long offset = mailbox.poll(key.topic(), key.domainId());
//                    if (offset == null) {
//                        // no work to do; treat as handled
//                        return CompletableFuture.completedFuture(null);
//                    }
//                    inflight.incrementAndGet();
//                    return ProcessMessage.processMessage(ctxRef.get(), processorRef.get(), new Msg(key.domainId()), offset)
//                            .whenComplete((r, ex) -> {
//                                if (ex != null && root(ex) instanceof IllegalStateException) {
//                                    illegalStateSeen.increment();
//                                }
//                                inflight.decrementAndGet();
//                            }).thenApply(v -> null);
//                }
//        );
//
//        // ----- context & processor (after runner created; we pass ctx/proc via refs used above) -----
//        record Msg(String domainId) {
//        }
//        Function<Msg, String> domainIdFn = Msg::domainId;
//
//        // references to avoid capture-before-init in the runner lambda above
//
//        final AtomicReference<ProcessMessageContext<JXPathContext, Msg, Long>> ctxRef = new AtomicReference<>();
//        final AtomicReference<ProcessMessage<JXPathContext, Msg, Long, String>> processorRef = new AtomicReference<>();
//        final AtomicInteger inflight = new AtomicInteger(0);
//        final LongAdder illegalStateSeen = new LongAdder();
//
//        // ProcessMessage impl with sync failure injection
//        ProcessMessage<JXPathContext, Msg, Long, String> processor = (current, msg) -> {
//            if (rnd.nextInt(PROCESS_SYNC_FAIL_ONE_IN) == 0) {
//                throw new IllegalStateException("processor-sync-boom");
//            }
//            var events = List.<IOpticsEvent<JXPathContext>>of(s -> s); // one mutation event
//            var sideFx = List.of("ok");
//            return CompletableFuture.completedFuture(new SideEffectsAndCepStateMutations<>(events, sideFx));
//        };
//        processorRef.set(processor);
//
//        // context (buckets is our decorator that also mirrors into mailbox)
//        var ctx = new ProcessMessageContext<>(
//                TOPIC,
//                0L,         // start state for each domain
//                cepState,
//                lease,
//                buckets,
//                time,
//                domainIdFn
//        );
//        ctxRef.set(ctx);
//
//        // ----- driver counters -----
//        LongAdder produced = new LongAdder();
//        ConcurrentHashMap<String, Long> nextOffset = new ConcurrentHashMap<>();
//        for (int i = 0; i < NUM_DOMAINS; i++) nextOffset.put("d-" + i, -1L); // start from offset 0 on first increment
//
//        // start runner
//        runner.start();
//
//        // ----- generate initial stream for DURATION_SECONDS -----
//        for (int s = 0; s < DURATION_SECONDS; s++) {
//            long batchStart = System.currentTimeMillis();
//            CountDownLatch latch = new CountDownLatch(EVENTS_PER_SECOND);
//            for (int i = 0; i < EVENTS_PER_SECOND; i++) {
//                submitPool.submit(() -> {
//                    try {
//                        String domain = "d-" + rnd.nextInt(NUM_DOMAINS);
//                        long offset = nextOffset.compute(domain, (k, v) -> (v == null ? 0L : v + 1L));
//                        produced.increment();
//                        // enqueue into mailbox + real buckets stage-0; runner will pop and call ProcessMessage
//                        mailbox.offer(TOPIC, domain, offset);
//                        buckets.addToRetryBucket(TOPIC, domain, offset, time.currentTimeMillis(), 0);
//                    } finally {
//                        latch.countDown();
//                    }
//                });
//            }
//            latch.await();
//            long elapsed = System.currentTimeMillis() - batchStart;
//            long sleep = 1_000L - elapsed;
//            if (sleep > 0) Thread.sleep(sleep);
//        }
//
//        // ----- drain -----
//        long drainDeadline = System.currentTimeMillis() + 15_000;
//        while ((inflight.get() > 0 || mailbox.totalQueued() > 0) &&
//                System.currentTimeMillis() < drainDeadline) {
//            Thread.sleep(50);
//        }
//
//        // shutdown
//        runner.close();
//        scheduler.shutdown();
//        submitPool.shutdown();
//        scheduler.awaitTermination(2, TimeUnit.SECONDS);
//        submitPool.awaitTermination(2, TimeUnit.SECONDS);
//
//        // ----- results -----
//        long successes = lease.successCount.sum();
//        long giveUps = lease.giveUpCount.sum();
//        long total = produced.sum();
//
//        System.out.println("---- Process + Buckets Soak ----");
//        System.out.printf("Produced         : %d%n", total);
//        System.out.printf("Successes        : %d%n", successes);
//        System.out.printf("GiveUps          : %d%n", giveUps);
//        System.out.printf("IllegalState seen: %d%n", illegalStateSeen.sum());
//        System.out.printf("Mailbox queued   : %d%n", mailbox.totalQueued());
//        System.out.println();
//
//        if (illegalStateSeen.sum() > 0) {
//            throw new AssertionError("Observed IllegalStateException(s) during soak");
//        }
//        if (successes + giveUps != total) {
//            throw new AssertionError("Mismatch: successes + giveUps != produced");
//        }
//    }
//
//    // ---------------- helpers used only here ----------------
//
//    /**
//     * Wraps WorkLease to count successes and give-ups (willRetry==false).
//     */
//    static final class CountingWorkLease implements WorkLease {
//        final WorkLease delegate;
//        final LongAdder successCount = new LongAdder();
//        final LongAdder giveUpCount = new LongAdder();
//
//        CountingWorkLease(WorkLease d) {
//            this.delegate = d;
//        }
//
//        @Override
//        public AcquireResult tryAcquire(String domainId, long offset) {
//            return delegate.tryAcquire(domainId, offset);
//        }
//
//        @Override
//        public SuceedResult succeed(String domainId, String token) {
//            var r = delegate.succeed(domainId, token);
//            successCount.increment();
//            // note: successes counted here (per call), which is once per offset
//            return r;
//        }
//
//        @Override
//        public FailResult fail(String domainId, String token) {
//            var r = delegate.fail(domainId, token);
//            if (!r.willRetry()) giveUpCount.increment();
//            return r;
//        }
//    }
//
//    /**
//     * Deterministic token generator for reproducibility.
//     */
//    static final class DeterministicTokenGenerator implements ITokenGenerator {
//        private final AtomicInteger seq = new AtomicInteger();
//
//        @Override
//        public String next(String domainId, long offset) {
//            return domainId + ":" + offset + "#" + seq.getAndIncrement();
//        }
//    }
//
//    /**
//     * In-memory CepState: per-domain Long counter; async failure injection on mutate.
//     */
//    static final class InMemoryCepState implements CepState<JXPathContext, Long> {
//        final ConcurrentHashMap<String, Long> state = new ConcurrentHashMap<>();
//        final Random rnd;
//
//        InMemoryCepState(Random rnd) {
//            this.rnd = rnd;
//        }
//
//        @Override
//        public CompletionStage<Long> get(String domainId, Long startState) {
//            return CompletableFuture.completedFuture(state.getOrDefault(domainId, startState));
//        }
//
//        @Override
//        public CompletionStage<Void> mutate(String domainId, List<IOpticsEvent<JXPathContext>> events) {
//            if (MUTATE_ASYNC_FAIL_ONE_IN > 0 && rnd.nextInt(MUTATE_ASYNC_FAIL_ONE_IN) == 0) {
//                return CompletableFuture.failedFuture(new RuntimeException("mutate-fail"));
//            }
//            // add #events to the counter (1 per message in this soak)
//            state.merge(domainId, (long) Math.max(1, events.size()), Long::sum);
//            return CompletableFuture.completedFuture(null);
//        }
//    }
//
//    /**
//     * Mailbox keeps per-(topic,domain) FIFO of offsets; runner pops one offset per key execution.
//     */
//    static final class OffsetMailbox {
//        private final ConcurrentHashMap<String, ConcurrentLinkedQueue<Long>> q = new ConcurrentHashMap<>();
//
//        private static String key(String topic, String domain) {
//            return topic + "|" + domain;
//        }
//
//        void offer(String topic, String domain, long offset) {
//            q.computeIfAbsent(key(topic, domain), k -> new ConcurrentLinkedQueue<>()).offer(offset);
//        }
//
//        Long poll(String topic, String domain) {
//            var queue = q.get(key(topic, domain));
//            return (queue == null) ? null : queue.poll();
//        }
//
//        int totalQueued() {
//            return q.values().stream().mapToInt(ConcurrentLinkedQueue::size).sum();
//        }
//    }
//
//    /**
//     * Decorator that mirrors offsets into mailbox and delegates to the real buckets.
//     */
//    static final class MailboxMirroringBuckets implements IRetryBuckets {
//        private final IRetryBuckets delegate;
//        private final OffsetMailbox mailbox;
//
//        MailboxMirroringBuckets(IRetryBuckets delegate, OffsetMailbox mailbox) {
//            this.delegate = delegate;
//            this.mailbox = mailbox;
//        }
//
//        @Override
//        public boolean addToRetryBucket(String topic, String domainId, Object offsetObj, long nowMillis, int retryCount) {
//            long offset = (offsetObj instanceof Number)
//                    ? ((Number) offsetObj).longValue()
//                    : Long.parseLong(String.valueOf(offsetObj));
//            mailbox.offer(topic, domainId, offset);
//            return delegate.addToRetryBucket(topic, domainId, offset, nowMillis, retryCount);
//        }
//    }
//
//    private static Throwable root(Throwable t) {
//        Throwable x = t;
//        while (x instanceof CompletionException || x instanceof ExecutionException) {
//            if (x.getCause() == null) break;
//            x = x.getCause();
//        }
//        return x;
//    }
//}
