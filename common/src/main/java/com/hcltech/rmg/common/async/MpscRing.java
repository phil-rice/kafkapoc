package com.hcltech.rmg.common.async;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.Objects;

/**
 * Lock-free MPSC ring buffer (many producers, single consumer).
 *
 * Implementation uses Vyukov’s algorithm with per-slot sequence numbers.
 * Capacity must be a power of two.
 * Now corrected for full release semantics to guarantee visibility across CPUs.
 */
public final class MpscRing<In, Out> implements IMpscRing<In, Out> {

    // Completion kind tags
    private static final byte TAG_EMPTY   = 0;
    private static final byte TAG_SUCCESS = 1;
    private static final byte TAG_FAILURE = 2;

    private final int capacity;
    private final int mask;

    // Producer ticket (tail): many producers update atomically
    private volatile long tail = 0L;

    // Consumer head: single reader only
    private long head = 0L;

    // Per-slot sequence numbers (publication protocol)
    private final long[] seq;

    // Parallel arrays for payload
    private final Object[] inArr;
    private final long[]   corrArr;
    private final Object[] outArr;
    private final Object[] errArr;
    private final byte[]   tagArr;

    // VarHandles
    private static final VarHandle VH_TAIL;
    private static final VarHandle VH_SEQ_A;
    private static final VarHandle VH_IN_A;
    private static final VarHandle VH_OUT_A;
    private static final VarHandle VH_ERR_A;
    private static final VarHandle VH_TAG_A;

    static {
        try {
            MethodHandles.Lookup l = MethodHandles.lookup();
            VH_TAIL  = l.findVarHandle(MpscRing.class, "tail", long.class);
            VH_SEQ_A = MethodHandles.arrayElementVarHandle(long[].class);
            VH_IN_A  = MethodHandles.arrayElementVarHandle(Object[].class);
            VH_OUT_A = MethodHandles.arrayElementVarHandle(Object[].class);
            VH_ERR_A = MethodHandles.arrayElementVarHandle(Object[].class);
            VH_TAG_A = MethodHandles.arrayElementVarHandle(byte[].class);
        } catch (ReflectiveOperationException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    public MpscRing(int capacityPow2) {
        if (capacityPow2 <= 0 || Integer.bitCount(capacityPow2) != 1)
            throw new IllegalArgumentException("capacity must be a power of two > 0");
        this.capacity = capacityPow2;
        this.mask     = capacityPow2 - 1;

        this.seq     = new long[capacity];
        this.inArr   = new Object[capacity];
        this.corrArr = new long[capacity];
        this.outArr  = new Object[capacity];
        this.errArr  = new Object[capacity];
        this.tagArr  = new byte[capacity];

        // Initialize per-slot sequence numbers
        for (int i = 0; i < capacity; i++) {
            VH_SEQ_A.setRelease(seq, i, (long) i);
        }
    }

    // ----------------- Producer API -----------------

    @Override
    public boolean offerSuccess(In in, long corrId, Out out) {
        Objects.requireNonNull(in, "in");
        Objects.requireNonNull(out, "out");
        return offerInternal(in, corrId, out, null, TAG_SUCCESS);
    }

    @Override
    public boolean offerFailure(In in, long corrId, Throwable error) {
        Objects.requireNonNull(in, "in");
        Objects.requireNonNull(error, "error");
        return offerInternal(in, corrId, null, error, TAG_FAILURE);
    }

    private boolean offerInternal(In in, long corrId, Out out, Throwable err, byte tag) {
        // 1) claim ticket
        long ticket = (long) VH_TAIL.getAndAdd(this, 1L);
        int idx = (int) (ticket & mask);

        // 2) wait for slot free
        for (;;) {
            long s = (long) VH_SEQ_A.getAcquire(seq, idx);
            if (s == ticket) break;
            Thread.onSpinWait();
        }

        // 3) publish payload (release writes to ensure visibility)
        VH_IN_A.setRelease(inArr, idx, in);
        corrArr[idx] = corrId;
        if (tag == TAG_SUCCESS) {
            VH_OUT_A.setRelease(outArr, idx, out);
            VH_ERR_A.setRelease(errArr, idx, null);
        } else {
            VH_OUT_A.setRelease(outArr, idx, null);
            VH_ERR_A.setRelease(errArr, idx, err);
        }
        VH_TAG_A.setRelease(tagArr, idx, tag);

        // 4) publish ready sequence
        VH_SEQ_A.setRelease(seq, idx, ticket + 1L);
        return true;
    }

    // ----------------- Consumer API -----------------

    @Override
    public int drain(Handler<In, Out> handler) {
        int n = 0;
        while (pollOnceInternal(handler)) n++;
        return n;
    }

    @Override
    public IMpscRing.Event<In, Out> pollOne() {
        final Holder<In,Out> h = new Holder<>();
        if (pollOnceInternal(new Handler<>() {
            @Override public void onSuccess(In in, long corrId, Out out) {
                h.ev = new IMpscRing.Event<>(true, in, corrId, out, null);
            }
            @Override public void onFailure(In in, long corrId, Throwable error) {
                h.ev = new IMpscRing.Event<>(false, in, corrId, null, error);
            }
        })) return h.ev;
        return null;
    }

    private boolean pollOnceInternal(Handler<In, Out> handler) {
        int idx = (int) (head & mask);
        long ready = head + 1L;

        long s = (long) VH_SEQ_A.getAcquire(seq, idx);
        if (s != ready) return false;

        @SuppressWarnings("unchecked")
        In in = (In) VH_IN_A.getAcquire(inArr, idx);
        long corrId = corrArr[idx];
        byte tag = (byte) VH_TAG_A.getAcquire(tagArr, idx);

        if (tag == TAG_SUCCESS) {
            @SuppressWarnings("unchecked")
            Out out = (Out) VH_OUT_A.getAcquire(outArr, idx);
            handler.onSuccess(in, corrId, out);
        } else {
            Throwable err = (Throwable) VH_ERR_A.getAcquire(errArr, idx);
            handler.onFailure(in, corrId, err);
        }

        // clear and free slot
        VH_IN_A.setRelease(inArr, idx, null);
        VH_OUT_A.setRelease(outArr, idx, null);
        VH_ERR_A.setRelease(errArr, idx, null);
        VH_TAG_A.setRelease(tagArr, idx, TAG_EMPTY);
        VH_SEQ_A.setRelease(seq, idx, head + capacity);
        head++;
        return true;
    }

    private static final class Holder<In,Out> { IMpscRing.Event<In,Out> ev; }
}
