package com.hcltech.rmg.cepstate.worklease;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

public final class MemoryWorkLease<Msg> implements WorkLease<Msg> {

    private static final class LeaseState<M> {
        String token;                       // non-null while leased
        final Deque<M> q = new ArrayDeque<>(); // backlog (future items only)
    }

    private final Map<String, LeaseState<Msg>> byDomain = new ConcurrentHashMap<>();
    private final ITokenGenerator tokenGen;
    private final EnqueueObserver<Msg> observer; // nullable, test-only
    private final ConcurrentLinkedQueue<String> trash = new ConcurrentLinkedQueue<>();

    public MemoryWorkLease(ITokenGenerator tokenGen) {
        this(tokenGen, null);
    }

    public MemoryWorkLease(ITokenGenerator tokenGen, EnqueueObserver<Msg> observer) {
        this.tokenGen = Objects.requireNonNull(tokenGen, "tokenGen");
        this.observer = observer;
    }

    private void drainTrash() {
        int max = trash.size();
        for (int i = 0; i < max; i++) {
            String d = trash.poll();
            if (d == null) break;

            // Atomically: if still idle, remove; else keep it
            byDomain.computeIfPresent(d, (key, s) -> {
                synchronized (s) {
                    boolean canBeDeleted = s.q.isEmpty() && s.token == null;
                    return canBeDeleted ? null : s; // returning null removes the entry
                }
            });
        }
    }

    @Override
    public String tryAcquire(String domainId, Msg message) {
        Objects.requireNonNull(domainId, "domainId");
        Objects.requireNonNull(message, "message");
        drainTrash();

        // get-or-create the per-domain state (monitor)
        LeaseState<Msg> s = byDomain.computeIfAbsent(domainId, k -> new LeaseState<>());
        synchronized (s) {
            if (s.token == null && s.q.isEmpty()) {
                s.token = tokenGen.next(domainId); // start new lease epoch
//                System.out.println("Acquire: " + domainId + " → " + s.token + " domain id hash" + domainId.hashCode());
                return s.token;
            }

            s.q.addLast(message);  // executor holds current payload; we store future
            if (observer != null) {
                observer.onEnqueued(domainId, message);
            }
            return null;
        }
    }

    @Override
    public HandBackTokenResult<Msg> succeed(String domainId, String token) {
        return complete(domainId, token);
    }

    @Override
    public HandBackTokenResult<Msg> fail(String domainId, String token) {
        return complete(domainId, token);
    }

    private HandBackTokenResult<Msg> complete(String domainId, String token) {
        Objects.requireNonNull(domainId, "domainId");
        Objects.requireNonNull(token, "token");

        LeaseState<Msg> s = byDomain.get(domainId);
        if (s == null) return HandBackTokenResult.noopIdle();

        synchronized (s) {
            if (!token.equals(s.token)) {
                return HandBackTokenResult.noopWrongToken(); // idempotent no-op
            }

            if (!s.q.isEmpty()) {
                Msg nextMsg = s.q.removeFirst();
                String nextTok = tokenGen.next(domainId); // advance epoch
                s.token = nextTok;
//                System.out.println("  Give: " + domainId + " → " + s.token+ " domain id hash" + domainId.hashCode());
                return HandBackTokenResult.handedOff(nextMsg, nextTok);
            }

            // queue empty → end lease epoch; schedule cleanup via trash
//            System.out.println("  Release: " + domainId+ " domain id hash" + domainId.hashCode());
            s.token = null;
            trash.offer(domainId);             // <-- ensure eventual removal in drainTrash()
            return HandBackTokenResult.ended();
        }
    }
}
