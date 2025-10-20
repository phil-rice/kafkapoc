package com.hcltech.rmg.messages;

import com.hcltech.rmg.common.async.HasSeq;

/** HasSeq type class for Envelope: delegates to the inner ValueEnvelope. */
public final class HasSeqsForEnvelope implements HasSeq<Envelope<?, ?>> {

    public static final HasSeqsForEnvelope INSTANCE = new HasSeqsForEnvelope();

    private HasSeqsForEnvelope() {}

    @Override
    public long get(Envelope<?, ?> value) {
        return value.valueEnvelope().getSeq();
    }

    @Override
    public Envelope<?, ?> set(Envelope<?, ?> value, long seq) {
        value.valueEnvelope().setSeq(seq);
        return value;
    }

    /** Optional: typed accessor to avoid wildcards at call sites. */
    @SuppressWarnings("unchecked")
    public static <S, M> HasSeq<Envelope<S, M>> typed() {
        return (HasSeq<Envelope<S, M>>) (HasSeq<?>) INSTANCE;
    }
}
