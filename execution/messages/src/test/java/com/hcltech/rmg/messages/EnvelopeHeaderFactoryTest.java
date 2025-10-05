package com.hcltech.rmg.messages;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

public final class EnvelopeHeaderFactoryTest {

//    // Minimal stub to avoid broker deps
//    static final class DummyRawMessage implements RawMessage {
//        private final String v;
//        DummyRawMessage(String v) { this.v = v; }
//        @Override public String rawValue() { return v; }
//        @Override public long brokerTimestamp() { return 0; }
//        @Override public long processingTimestamp() { return 0; }
//        @Override public int partition() { return 0; }
//        @Override public String traceparent() { return null; }
//        @Override public String tracestate() { return null; }
//        @Override public String baggage() { return null; }
//    }
//
//    static final class DummyCepState { final int n; DummyCepState(int n){ this.n = n; } }
//
//    private final EnvelopeHeaderFactory<DummyCepState> factory =
//            new InitialEnvelopeFactory<>("parcel");
//
//    @Test
//    @DisplayName("createEnvelopeHeaderAtStart: sets domainType/domainId/raw/cepState; others null")
//    void create_ok() {
//        var raw = new DummyRawMessage("<xml/>");
//        var cep = new DummyCepState(42);
//
//        var hdr = factory.createEnvelopeHeaderAtStart(raw, "P-123", cep);
//
//        assertEquals("parcel", hdr.domainType());
//        assertEquals("P-123", hdr.domainId());
//        assertNull(hdr.eventType(), "eventType is not known at start");
//        assertSame(raw, hdr.rawMessage(), "keeps same RawMessage reference");
//        assertNull(hdr.parameters(), "parameters null at start");
//        assertNull(hdr.config(),     "config null at start");
//        assertSame(cep, hdr.cepState(), "keeps same CEP state reference");
//    }
//
//    @Test
//    @DisplayName("createEnvelopeHeaderAtStart: null rawMessage → NPE with message")
//    void null_raw_throws() {
//        var cep = new DummyCepState(1);
//        var ex = assertThrows(NullPointerException.class,
//                () -> factory.createEnvelopeHeaderAtStart(null, "P-1", cep));
//        assertTrue(ex.getMessage().contains("rawMessage"));
//    }
//
//    @Test
//    @DisplayName("createEnvelopeHeaderAtStart: null domainId → NPE with message")
//    void null_domain_throws() {
//        var raw = new DummyRawMessage("x");
//        var cep = new DummyCepState(1);
//        var ex = assertThrows(NullPointerException.class,
//                () -> factory.createEnvelopeHeaderAtStart(raw, null, cep));
//        assertTrue(ex.getMessage().contains("domainId"));
//    }
//
//    @Test
//    @DisplayName("createEnvelopeHeaderAtStart: null cepState → NPE with message")
//    void null_cep_throws() {
//        var raw = new DummyRawMessage("x");
//        var ex = assertThrows(NullPointerException.class,
//                () -> factory.createEnvelopeHeaderAtStart(raw, "P-1", null));
//        assertTrue(ex.getMessage().contains("cepState"));
//    }
}
