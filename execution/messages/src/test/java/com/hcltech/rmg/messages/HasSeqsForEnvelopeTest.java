package com.hcltech.rmg.messages;

import com.hcltech.rmg.common.async.HasSeq;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class HasSeqsForEnvelopeTest {

    private static EnvelopeHeader<Object> header(String domainId) {
        // parameters = null, config = null, cargo = null are legal
        return new EnvelopeHeader<>(
                "domainType",
                "eventType",
                new RawMessage("raw", domainId, 0L, 0L, 0, 0L, "", "", ""),
                null,   // Parameters
                null,   // BehaviorConfig
                null    // cargo
        );
    }

    /** Build a ValueEnvelope using the 4-arg ctor; cepState=null, empty modifications. */
    private static ValueEnvelope<Object, String> ve(String domainId, String value) {
        return new ValueEnvelope<>(header(domainId), value, null, new ArrayList<>());
    }

    @Test
    void get_and_set_delegate_to_inner_ValueEnvelope() {
        HasSeq<Envelope<?, ?>> tc = HasSeqsForEnvelope.INSTANCE;

        var env = ve("A", "msg"); // ValueEnvelope IS an Envelope

        assertEquals(0L, tc.get(env));
        tc.set(env, 42L);
        assertEquals(42L, tc.get(env));
        // sanity: inner VE actually changed
        assertEquals(42L, env.valueEnvelope().getSeq());
    }

    @Test
    void works_with_ErrorEnvelope_and_RetryEnvelope() {
        HasSeq<Envelope<?, ?>> tc = HasSeqsForEnvelope.INSTANCE;

        var base = ve("B", "oops");
        var err = new ErrorEnvelope<>(base, "stageX", List.of("E1"));
        assertEquals(0L, tc.get(err));
        tc.set(err, 7L);
        assertEquals(7L, tc.get(err));
        assertEquals(7L, err.valueEnvelope().getSeq());

        var base2 = ve("C", "retry");
        var retry = new RetryEnvelope<>(base2, "stageY", 2);
        assertEquals(0L, tc.get(retry));
        tc.set(retry, 19L);
        assertEquals(19L, tc.get(retry));
        assertEquals(19L, retry.valueEnvelope().getSeq());
    }
}
