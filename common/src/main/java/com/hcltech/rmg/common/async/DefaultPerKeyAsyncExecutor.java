package com.hcltech.rmg.common.async;

import java.util.Objects;
import java.util.concurrent.*;
import java.util.function.Function;

/**
 * Minimal-GC per-key async executor:
 * - Per-key cap K (no pending queue; caller backpressures when saturated).
 * - Subtask-wide cap via PermitManager M (tryAcquire before launch; release after ordered commit).
 * - Async success yields Out directly; async failure transformed to Out via FailureAdapter.
 * - Seq is managed via HasSeq type classes for In and Out; we propagate seq from input to Out.
 * - Inbox carries Out objects; ring enforces strict in-order delivery on the operator thread.
 */
public class DefaultPerKeyAsyncExecutor<In, Out> implements PerKeyAsyncExecutor<In, Out> {

    private final int K;                                      // per-key in-flight cap
    private final PermitManager permitManager;                // subtask-wide throttle (M)
    private final Function<In, CompletionStage<Out>> asyncFn; // success path produces final Out
    private final FailureAdapter<In, Out> failureAdapter;     // failure path maps (Throwable, In) -> Out
    private final HasSeq<In> inSeq;
    private final HasSeq<Out> outSeq;

    private final Executor executor;                          // owned or injected
    private final boolean shutdownExecutor;

    // Async threads push final Out here; operator thread drains on onWake()
    private final ConcurrentLinkedQueue<Out> inbox;

    // Strict in-order delivery buffer (private detail)
    private final CircularBufferWithCallback<Out> ring;

    // State (operator thread only)
    private long nextSeq = 0L;
    private int inFlight = 0;
    private volatile boolean finished = false;

    /**
     * @param perKeyCapK per-key concurrency cap K; ring capacity is rounded up to next power-of-two
     * @param executorOrNull shared executor or null to create an internal cached pool
     * @param asyncFn async function producing final Out on success; must not touch Flink state
     * @param failureAdapter converts (Throwable, In) into a final Out
     * @param permitManager subtask-wide permit manager (M)
     * @param inSeq type class for getting/setting sequence on In
     * @param outSeq type class for getting/setting sequence on Out
     */
    public DefaultPerKeyAsyncExecutor(
            int perKeyCapK,
            Executor executorOrNull,
            Function<In, CompletionStage<Out>> asyncFn,
            FailureAdapter<In, Out> failureAdapter,
            PermitManager permitManager,
            HasSeq<In> inSeq,
            HasSeq<Out> outSeq
    ) {
        if (perKeyCapK <= 0) throw new IllegalArgumentException("K must be > 0");

        int ringCap = 1;
        while (ringCap < perKeyCapK) ringCap <<= 1;

        this.K = perKeyCapK;
        this.asyncFn = Objects.requireNonNull(asyncFn, "asyncFn");
        this.failureAdapter = Objects.requireNonNull(failureAdapter, "failureAdapter");
        this.permitManager = Objects.requireNonNull(permitManager, "permitManager");
        this.inSeq = Objects.requireNonNull(inSeq, "inSeq");
        this.outSeq = Objects.requireNonNull(outSeq, "outSeq");
        this.inbox = new ConcurrentLinkedQueue<>();

        if (executorOrNull == null) {
            ExecutorService es = Executors.newCachedThreadPool(r -> {
                Thread t = new Thread(r, "perkey-async");
                t.setDaemon(true);
                return t;
            });
            this.executor = es;
            this.shutdownExecutor = true;
        } else {
            this.executor = executorOrNull;
            this.shutdownExecutor = false;
        }

        // IMPORTANT: release capacity exactly once.
        this.ring = new CircularBufferWithCallback<>(
                ringCap,
                out -> {
                    handleOrdered(out);      // may throw
                    inFlight--;
                    permitManager.release();
                },
                (out, ex) -> {
                    // handleOrdered threw → still free capacity to avoid deadlock
                    inFlight--;
                    permitManager.release();
                }
        );
    }

    /** Override to apply CEP mutations / emit results on the operator thread. Default: no-op. */
    protected void handleOrdered(Out out) {
        // Intentionally empty; override in a subclass to integrate with CEP & collectors.
    }

    @Override
    public AcceptResult execute(In input) {
        if (finished) return AcceptResult.REJECTED_FULL;

        // Per-key cap (no pending): caller must backpressure if saturated
        if (inFlight >= K) return AcceptResult.REJECTED_FULL;

        // Subtask-wide cap
        if (!permitManager.tryAcquire()) return AcceptResult.REJECTED_FULL;

        // Launch
        final long seq = nextSeq++;
        // Tag input with seq; support immutable inputs by allowing HasSeq.set to return a new instance
        final In taggedIn = inSeq.set(input, seq);
        inFlight++;
        launchAsync(taggedIn, seq);
        return AcceptResult.LAUNCHED;
    }

    private void launchAsync(In input, long seqFromInput) {
        try {
            CompletionStage<Out> cs = asyncFn.apply(input);
            cs.whenCompleteAsync((out, err) -> {
                if (finished) return;
                final Out rawOut = (err != null)
                        ? failureAdapter.onFailure(input, err)
                        : out;
                if (rawOut != null) {
                    // Tag output with input's seq (support immutable outputs by returning new instance)
                    final Out taggedOut = outSeq.set(rawOut, seqFromInput);
                    inbox.offer(taggedOut);
                }
            }, executor);
        } catch (Throwable t) {
            if (!finished) {
                Out rawOut = failureAdapter.onFailure(input, t);
                if (rawOut != null) {
                    final Out taggedOut = outSeq.set(rawOut, seqFromInput);
                    inbox.offer(taggedOut);
                }
            }
        }
    }

    @Override
    public void onWake() {
        for (Out out; (out = inbox.poll()) != null; ) {
            long seq = outSeq.get(out);
            ring.put(seq, out); // ring enforces in-order delivery and invokes handleOrdered
        }
    }

    @Override
    public boolean needsWake() {
        return inFlight > 0 || !inbox.isEmpty();
    }

    @Override
    public void finish() {
        finished = true;
        ring.finish(); // ignore late completions safely
        if (shutdownExecutor && executor instanceof ExecutorService es) {
            es.shutdownNow();
        }
    }
}
