package com.hcltech.rmg.common.async;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.Objects;
import java.util.function.BiConsumer;

/**
 * Lock-free multi-producer single-consumer ring.
 * Many producers offer events; a single consumer drains them on the operator thread.
 *
 * NOTE: This implementation deliberately does NOT store FR (ResultFuture) in the ring.
 *       Resolve FR on the consumer side using corrId.
 */
public final class MpscRing<FR, In, Out> implements IMpscRing<FR, In, Out> {

    private static final byte TAG_EMPTY   = 0;
    private static final byte TAG_SUCCESS = 1;
    private static final byte TAG_FAILURE = 2;

    private final int capacity;
    private final int mask;

    // producers update tail; single consumer reads head
    private volatile long tail = 0L; // MPSC
    private long head = 0L;          // SPSC

    private final long[]  seq;

    // NOTE: no FR array here
    private final Object[] inArr;
    private final String[] corrArr;
    private final Object[] outArr;
    private final Object[] errArr;
    private final byte[]   tagArr;

    private static final VarHandle VH_TAIL, VH_SEQ_A, VH_IN_A, VH_OUT_A, VH_ERR_A, VH_TAG_A;

    static {
        try {
            MethodHandles.Lookup l = MethodHandles.lookup();
            VH_TAIL = l.findVarHandle(MpscRing.class, "tail", long.class);
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
        this.mask = capacityPow2 - 1;

        this.seq     = new long[capacity];
        this.inArr   = new Object[capacity];
        this.corrArr = new String[capacity];
        this.outArr  = new Object[capacity];
        this.errArr  = new Object[capacity];
        this.tagArr  = new byte[capacity];

        for (int i = 0; i < capacity; i++) {
            VH_SEQ_A.setRelease(seq, i, (long) i);
        }
    }

    // ---- producers ----

    @Override
    public boolean offerSuccess(In in, String corrId, Out out) {
        Objects.requireNonNull(in,  "in");
        Objects.requireNonNull(out, "out");
        return offerInternal(in, corrId, out, null, TAG_SUCCESS);
    }

    @Override
    public boolean offerFailure(In in, String corrId, Throwable error) {
        Objects.requireNonNull(in,    "in");
        Objects.requireNonNull(error, "error");
        return offerInternal(in, corrId, null, error, TAG_FAILURE);
    }

    private boolean offerInternal(In in, String corrId, Out out, Throwable err, byte tag) {
        long ticket = (long) VH_TAIL.getAndAdd(this, 1L);
        int idx = (int) (ticket & mask);

        for (;;) {
            long s = (long) VH_SEQ_A.getAcquire(seq, idx);
            if (s == ticket) break;
            Thread.onSpinWait();
        }

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

        // publish
        VH_SEQ_A.setRelease(seq, idx, ticket + 1L);
        return true;
    }

    // ---- consumer ----

    @Override
    public int drain(BiConsumer<In, Out> onCompleteOrFailed, Handler<FR, In, Out> handler) {
        int n = 0;
        while (pollOnceInternal(onCompleteOrFailed, handler)) n++;
        return n;
    }

    private boolean pollOnceInternal(BiConsumer<In, Out> onCompleteOrFailed,
                                     Handler<FR, In, Out> handler) {
        int idx = (int) (head & mask);
        long ready = head + 1L;

        long s = (long) VH_SEQ_A.getAcquire(seq, idx);
        if (s != ready) return false;

        @SuppressWarnings("unchecked")
        In in = (In) VH_IN_A.getAcquire(inArr, idx);
        String corrId = corrArr[idx];
        byte tag = (byte) VH_TAG_A.getAcquire(tagArr, idx);

        if (tag == TAG_SUCCESS) {
            @SuppressWarnings("unchecked")
            Out out = (Out) VH_OUT_A.getAcquire(outArr, idx);
            // FR intentionally NOT stored in the ring; pass null (handler should ignore it)
            handler.onSuccess(null, onCompleteOrFailed, in, corrId, out);
        } else {
            Throwable err = (Throwable) VH_ERR_A.getAcquire(errArr, idx);
            handler.onFailure(null, onCompleteOrFailed, in, corrId, err);
        }

        // clear slot
        VH_IN_A.setRelease(inArr, idx, null);
        VH_OUT_A.setRelease(outArr, idx, null);
        VH_ERR_A.setRelease(errArr, idx, null);
        VH_TAG_A.setRelease(tagArr, idx, TAG_EMPTY);

        // move sequence forward for producers
        VH_SEQ_A.setRelease(seq, idx, head + capacity);
        head++;
        return true;
    }
}
