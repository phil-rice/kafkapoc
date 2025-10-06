package com.hcltech.rmg.cepstate.worklease;

import org.junit.jupiter.api.Test;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

public class MemoryWorkLeaseStressTest {

    record Message(String domain, int msg, String tokenOrNull) {
        Message(String domain, int msg) {
            this(domain, msg, null);
        }

        Message withToken(String token) {
            return new Message(domain, msg, token);
        }
    }

    @Test
    public void stressTest_queueWorkersInvariants() throws Exception {
        final int totalMessages = 100_000;
        final int domainCount = 50;
        final int workerCount = 100;

        List<String> domains = new ArrayList<>();
        for (int i = 0; i < domainCount; i++) domains.add("D" + i);

        MemoryWorkLease<Message> lease = new MemoryWorkLease<>(ITokenGenerator.incrementingGenerator());

        BlockingQueue<Message> messageQueue = new LinkedBlockingQueue<>();
        BlockingQueue<Message> workerQueue = new LinkedBlockingQueue<>();
        ThreadLocalRandom rnd = ThreadLocalRandom.current();

        // FIFO oracle: order messages were offered per domain
        Map<String, Queue<Integer>> expectedOrder = new ConcurrentHashMap<>();
        for (String d : domains) expectedOrder.put(d, new ConcurrentLinkedQueue<>());

        for (int id = 1; id <= totalMessages; id++) {
            String d = domains.get(rnd.nextInt(domainCount));
            expectedOrder.get(d).add(id);
            messageQueue.add(new Message(d, id));
        }

        // Invariant helpers
        Set<String> activeDomains = ConcurrentHashMap.newKeySet();
        AtomicInteger processedCount = new AtomicInteger(0);
        AtomicInteger dotCount = new AtomicInteger(0);

        // Surface worker exceptions
        AtomicReference<Throwable> firstError = new AtomicReference<>(null);
        ThreadFactory tf = r -> {
            Thread t = new Thread(r, "wl-worker");
            t.setDaemon(true);
            t.setUncaughtExceptionHandler((th, ex) -> firstError.compareAndSet(null, ex));
            return t;
        };
        ExecutorService pool = Executors.newFixedThreadPool(workerCount, tf);

        // Dispatcher: single thread pulls from messageQueue, calls tryAcquire, enqueues WorkerItem when token
        Thread dispatcher = new Thread(() -> {
            try {
                while (processedCount.get() < totalMessages) {
                    Message msg = messageQueue.poll(200, TimeUnit.MILLISECONDS);
                    if (msg == null) continue;

                    String token = lease.tryAcquire(msg.domain(), msg);
                    if (token == null) {
                        // queued internally; will be surfaced via succeed() hand-back later
                        continue;
                    }
                    workerQueue.add(msg.withToken(token));
                }
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
            } catch (Throwable t) {
                firstError.compareAndSet(null, t);
            }
        }, "wl-dispatcher");
        dispatcher.setDaemon(true);

        // Workers: consume WorkerItem, check invariants, succeed, re-enqueue hand-backs as new Messages
        AtomicInteger workerId = new AtomicInteger(0);
        Runnable worker = () -> {
            try {
                int id = workerId.incrementAndGet();
//                System.out.println("Worker " + id + " started " + id);
                while (processedCount.get() < totalMessages && firstError.get() == null) {
                    Message item = workerQueue.poll(200, TimeUnit.MILLISECONDS);
                    if (item == null) continue;              // <<< FIX: null-check to avoid NPE
                    if (item.tokenOrNull()== null) {
                        throw new AssertionError("Worker got item without token: " + item);
                    }

                    Thread.sleep(rnd.nextInt(5));
                    // Invariant: only one active per domain
                    if (!activeDomains.add(item.domain())) { // returns false if already present
                        throw new AssertionError("Concurrent processing in " + item.domain());
                    }

                    // Invariant: FIFO order preserved
                    Integer expected = expectedOrder.get(item.domain()).poll();
                    if (!Objects.equals(expected, item.msg())) {
                        throw new AssertionError("FIFO broken for " + item.domain()
                                + ": expected " + expected + " got " + item.msg());
                    }

                    int done = processedCount.incrementAndGet();
                    if (done % 50 == 0) {
                        System.out.print(".");
                        if (dotCount.incrementAndGet() % 50 == 0) {
                            System.out.println( done + "/" + totalMessages);
                        }
                    }

                    activeDomains.remove(item.domain());
                    HandBackTokenResult<Message> r = lease.succeed(item.domain(), item.tokenOrNull());
                    if (r.status() == CompletionStatus.HANDED_OFF) {
                        // Hand back: put the next Message onto the dispatcher’s queue (single-thread admission)
                        Message next = r.message();
                        if (next == null || r.token() == null) {
                            throw new AssertionError("HANDED_OFF must include message and token");
                        }
                        workerQueue.add(next.withToken(r.token()));
                    } else if (r.status() == CompletionStatus.NOOP_WRONG_TOKEN || r.status() == CompletionStatus.NOOP_IDLE) {
                        throw new AssertionError("Unexpected " + r.status() + " after having a token");
                    }

                }
            } catch (Throwable t) {

                firstError.compareAndSet(null, t);
            }
        };

        // Start everything
        for (int i = 0; i < workerCount; i++) pool.submit(worker);
        System.out.println("All workers started");
        dispatcher.start();
        System.out.println("dispatcher started " + processedCount.get() + "/" + totalMessages);

        // Progress / wait loop (println to force flush)
        while (processedCount.get() < totalMessages && firstError.get() == null) {
            Thread.sleep(200);
//            System.out.println("messageQ=" + messageQueue.size()
//                    + " workerQ=" + workerQueue.size()
//                    + " processed=" + processedCount.get() + "/" + totalMessages);
        }

        // Surface any error
        if (firstError.get() != null) {
            firstError.get().printStackTrace(System.out);
            fail("Worker/dispatcher failed: " + firstError.get());
        }

        // Shutdown
        pool.shutdownNow();
        pool.awaitTermination(10, TimeUnit.SECONDS);

        System.out.println("\nProcessed: " + processedCount.get());
        assertEquals(totalMessages, processedCount.get(), "all messages processed");
        for (String d : domains) {
            assertTrue(expectedOrder.get(d).isEmpty(), "Unprocessed messages for domain " + d);
        }
    }
}
