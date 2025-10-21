package com.hcltech.rmg.messages;

import com.hcltech.rmg.common.async.FailureAdapter;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

public class EnvelopeFailureAdapterTest {

    // ---------- helpers ----------
    private static EnvelopeHeader<Object> header(String domainId) {
        return new EnvelopeHeader<>(
                "test-domain",
                "test-event",
                new RawMessage("raw", domainId, 1L, 2L, 3, 4L, "tp", "ts", "bg"),
                null,
                null,
                Map.of()
        );
    }

    private static ValueEnvelope<Object, String> ve(String domainId, String data) {
        return new ValueEnvelope<>(header(domainId), data, null, new ArrayList<>());
    }

    // ---------- tests ----------

    @Test
    void defaultAdapter_buildsFreshErrorEnvelope_preservesHeaderAndData_andUsesOperatorId() {
        // Arrange
        final String operatorId = "AsyncCepProcessFunction#2@attempt1";
        FailureAdapter<Envelope<Object,String>, Envelope<Object,String>> adapter =
                EnvelopeFailureAdapter.defaultAdapter(operatorId);

        ValueEnvelope<Object,String> inVE = ve("D-123", "payload-xyz");
        // mutate seq on input VE (adapter must NOT rely on this; it should not touch seq)
        inVE.setSeq(777);

        Envelope<Object,String> in = inVE; // ValueEnvelope implements Envelope
        RuntimeException boom = new RuntimeException("primary");

        // Act
        Envelope<Object,String> out = adapter.onFailure(in, boom);

        // Assert
        assertInstanceOf(ErrorEnvelope.class, out, "adapter must return ErrorEnvelope on failure");
        ErrorEnvelope<Object,String> errOut = (ErrorEnvelope<Object,String>) out;

        // stageName is operator id
        assertEquals(operatorId, errOut.stageName());

        // envelope is fresh (no aliasing)
        assertNotSame(in.valueEnvelope(), errOut.valueEnvelope(), "must build a fresh ValueEnvelope (no aliasing)");

        // header & data preserved
        assertEquals(in.valueEnvelope().header(), errOut.valueEnvelope().header(), "header must be preserved");
        assertEquals("payload-xyz", errOut.valueEnvelope().data(), "data must be preserved");

        // seq left as default (0) — executor will tag it later
        assertEquals(0L, errOut.valueEnvelope().getSeq(), "adapter must not set seq (left for executor)");

        // cepStateModifications in the new VE starts empty
        assertTrue(errOut.valueEnvelope().cepStateModifications().isEmpty(), "modifications list should be empty on failure");

        // errors list present & non-empty, includes class and message
        assertNotNull(errOut.errors());
        assertFalse(errOut.errors().isEmpty());
        assertTrue(errOut.errors().get(0).contains("RuntimeException"), "first error entry should contain class name");
        assertTrue(errOut.errors().get(0).contains("primary"), "first error entry should contain message");
    }

    @Test
    void defaultAdapter_compactsDeepCauseChain_andDotsWhenTruncated() {
        // Arrange
        final String operatorId = "op#0@attempt3";
        FailureAdapter<Envelope<Object,String>, Envelope<Object,String>> adapter =
                EnvelopeFailureAdapter.defaultAdapter(operatorId);

        // Build a 4-deep chain to trigger truncation with "..."
        Throwable t3 = new IllegalStateException("root");
        Throwable t2 = new RuntimeException("mid", t3);
        Throwable t1 = new IllegalArgumentException("top", t2);
        Throwable t0 = new RuntimeException("outer", t1);

        Envelope<Object,String> in = ve("D-999", "data");

        // Act
        ErrorEnvelope<Object,String> out = (ErrorEnvelope<Object,String>) adapter.onFailure(in, t0);

        // Assert: up to 3 entries + "..." truncation marker
        List<String> errs = out.errors();
        assertTrue(errs.size() >= 3, "should include at least three compact error lines");
        // First should be outer class & message
        assertTrue(errs.get(0).contains("RuntimeException"), "first entry must include class name");
        assertTrue(errs.get(0).contains("outer"), "first entry must include outer message");
        // Should indicate truncation
        assertEquals("...", errs.get(errs.size() - 1), "last entry should be truncation marker '...'");
    }

    @Test
    void defaultAdapter_doesNotAliasInputEnvelope_mutationsOnInputDoNotAffectOutput() {
        final String operatorId = "unique#id";
        FailureAdapter<Envelope<Object,String>, Envelope<Object,String>> adapter =
                EnvelopeFailureAdapter.defaultAdapter(operatorId);

        ValueEnvelope<Object,String> inVE = ve("D-1", "original");
        Envelope<Object,String> in = inVE;

        Envelope<Object,String> first = adapter.onFailure(in, new RuntimeException("x"));
        // mutate input AFTER failure mapping:
        inVE.setData("mutated");

        // The output must remain "original"
        assertEquals("original", first.valueEnvelope().data(), "output must not be affected by later input mutations");
    }

    @Test
    void timeout_maps_to_RetryEnvelope_and_preserves_header_and_data() {
        final String operatorId = "op-timeout";
        FailureAdapter<Envelope<Object,String>, Envelope<Object,String>> adapter =
                EnvelopeFailureAdapter.defaultAdapter(operatorId);

        Envelope<Object,String> in = ve("D-42", "payload");
        long elapsed = 123_456L;

        Envelope<Object,String> out = adapter.onTimeout(in, elapsed);

        assertInstanceOf(RetryEnvelope.class, out, "timeout should map to RetryEnvelope");
        RetryEnvelope<Object,String> retry = (RetryEnvelope<Object,String>) out;

        // If RetryEnvelope exposes stageName(), assert it; otherwise comment out the next line.
        assertEquals(operatorId, retry.stageName(), "stageName should be operator id");

        // Fresh VE, header/data preserved, mods empty, seq untouched
        assertNotSame(in.valueEnvelope(), retry.valueEnvelope());
        assertEquals(in.valueEnvelope().header(), retry.valueEnvelope().header());
        assertEquals("payload", retry.valueEnvelope().data());
        assertTrue(retry.valueEnvelope().cepStateModifications().isEmpty());
        assertEquals(0L, retry.valueEnvelope().getSeq());
    }

    @Test
    void null_arguments_are_rejected() {
        FailureAdapter<Envelope<Object,String>,Envelope<Object,String>> adapter =
                EnvelopeFailureAdapter.defaultAdapter("id");

        Envelope<Object,String> env = ve("D-1","x");
        assertThrows(NullPointerException.class, () -> adapter.onFailure(null,new RuntimeException()));
        assertThrows(NullPointerException.class, () -> adapter.onFailure(env,null));
        assertThrows(NullPointerException.class, () -> adapter.onTimeout(null,1L));
    }

    @Test
    void operatorId_must_not_be_null() {
        assertThrows(NullPointerException.class, () -> EnvelopeFailureAdapter.defaultAdapter(null));
    }
}
