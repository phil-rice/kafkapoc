// src/test/java/com/hcltech/rmg/messages/InitialEnvelopeFactoryTest.java
package com.hcltech.rmg.messages;

import com.hcltech.rmg.common.errorsor.ErrorsOr;
import com.hcltech.rmg.config.config.Config;
import com.hcltech.rmg.config.config.RootConfig;
import com.hcltech.rmg.parameters.ParameterExtractor;
import com.hcltech.rmg.parameters.Parameters;
import com.hcltech.rmg.xml.XmlTypeClass;
import org.junit.jupiter.api.Test;

import java.io.InputStream;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/**
 * Tests InitialEnvelopeFactory using tiny fakes for XmlTypeClass + ParameterExtractor,
 * and Mockito only for 'blind' types (Parameters, Config, RawMessage).
 */
class InitialEnvelopeFactoryTest {

    // ---- Tiny fakes ---------------------------------------------------------

    /** Fake XmlTypeClass that always succeeds with a given parsed map. */
    private static final class FakeXmlPass implements XmlTypeClass<Object> {
        private final Map<String, Object> parsed;
        FakeXmlPass(Map<String, Object> parsed) { this.parsed = parsed; }

        @Override public Object loadSchema(String schemaName, InputStream schemaStream) { return new Object(); }
        @Override public ErrorsOr<Map<String, Object>> parseAndValidate(String xml, Object schema) { return ErrorsOr.lift(parsed); }
        @Override public ErrorsOr<String> extractId(String rawString, List<String> idPath) { return ErrorsOr.error("unused"); }
    }

    /** Fake XmlTypeClass that always fails with a given error. */
    private static final class FakeXmlFail implements XmlTypeClass<Object> {
        private final String err;
        FakeXmlFail(String err) { this.err = err; }

        @Override public Object loadSchema(String schemaName, InputStream schemaStream) { return new Object(); }
        @Override public ErrorsOr<Map<String, Object>> parseAndValidate(String xml, Object schema) { return ErrorsOr.error(err); }
        @Override public ErrorsOr<String> extractId(String rawString, List<String> idPath) { return ErrorsOr.error("unused"); }
    }

    /** Fake ParameterExtractor that returns a pre-made Parameters value/errors. */
    private static final class FakeParamExtractor implements ParameterExtractor {
        private final ErrorsOr<Parameters> result;
        FakeParamExtractor(ErrorsOr<Parameters> result) { this.result = result; }
        @Override
        public ErrorsOr<Parameters> parameters(Map<String, Object> input, String eventType, String domainType, String domainId) {
            return result;
        }
    }

    // ---- Tests --------------------------------------------------------------

    @Test
    void happyPath_buildsValueEnvelope() {
        String domainId   = "D-123";
        String schemaName = "schemas/order.xsd";

        Map<String, Object> parsed = Map.of("id", "ABC", "amount", 10);
        XmlTypeClass<Object> xml = new FakeXmlPass(parsed);

        Object schema = new Object();
        Map<String, Object> nameToSchema = Map.of(schemaName, schema);
        RootConfig root = new RootConfig(null, schemaName);

        Parameters params = mock(Parameters.class);
        ParameterExtractor paramEx = new FakeParamExtractor(ErrorsOr.lift(params));

        // Extractors (new interfaces)
        IEventTypeExtractor eventTypeExtractor = message -> "OrderCreated";
        IDomainTypeExtractor domainTypeExtractor = message -> "orders";

        Config cfg = mock(Config.class); // behaviorConfig() not asserted
        Map<String, Config> keyToConfig = Map.of("OrderCreated", cfg);

        InitialEnvelopeFactory<Object, String> sut =
                new InitialEnvelopeFactory<>(paramEx, nameToSchema, xml, eventTypeExtractor, domainTypeExtractor, keyToConfig, root);

        RawMessage raw = mock(RawMessage.class);
        when(raw.rawValue()).thenReturn("<Order><Id>ABC</Id></Order>");

        Envelope<String, Map<String, Object>> env = sut.createEnvelopeHeaderAtStart(raw, domainId);

        assertTrue(env instanceof ValueEnvelope, "Expected ValueEnvelope");

        @SuppressWarnings("unchecked")
        ValueEnvelope<String, Map<String, Object>> ve = (ValueEnvelope<String, Map<String, Object>>) env;

        // payload
        assertEquals(parsed, ve.data());

        // header fields
        EnvelopeHeader<String> h = ve.header();
        assertEquals("orders", h.domainType());
        assertEquals(domainId, h.domainId());
        assertEquals("OrderCreated", h.eventType());
        assertSame(raw, h.rawMessage());
        assertSame(params, h.parameters());
    }

    @Test
    void parseError_recoversToErrorEnvelope_withEmptyMapPayload() {
        String domainId   = "D-ERR";
        String schemaName = "schemas/order.xsd";

        XmlTypeClass<Object> xml = new FakeXmlFail("xml invalid");
        Object schema = new Object();
        Map<String, Object> nameToSchema = Map.of(schemaName, schema);
        RootConfig root = new RootConfig(null, schemaName);

        ParameterExtractor paramEx = new FakeParamExtractor(ErrorsOr.error("unused"));
        IEventTypeExtractor eventTypeExtractor = message -> "X";
        IDomainTypeExtractor domainTypeExtractor = message -> "orders";
        Map<String, Config> keyToConfig = Map.of();

        InitialEnvelopeFactory<Object, String> sut =
                new InitialEnvelopeFactory<>(paramEx, nameToSchema, xml, eventTypeExtractor, domainTypeExtractor, keyToConfig, root);

        RawMessage raw = mock(RawMessage.class);
        when(raw.rawValue()).thenReturn("<broken/>");

        Envelope<String, Map<String, Object>> env = sut.createEnvelopeHeaderAtStart(raw, domainId);

        assertTrue(env instanceof ErrorEnvelope, "Expected ErrorEnvelope");

        @SuppressWarnings("unchecked")
        ErrorEnvelope<String, Map<String, Object>> ee = (ErrorEnvelope<String, Map<String, Object>>) env;

        // inner value envelope crafted in recover(): header with null eventType, payload = Map.of()
        ValueEnvelope<String, Map<String, Object>> inner = ee.valueEnvelope();
        EnvelopeHeader<String> h = inner.header();
        assertNull(h.eventType(), "eventType should be null in error header");
        assertSame(raw, h.rawMessage());
        assertEquals(Map.of(), inner.data(), "recover payload should be empty Map");
    }

    @Test
    void missingConfig_recoversToErrorEnvelope_withEmptyMapPayload() {
        String domainId   = "D-MISS-CFG";
        String schemaName = "schemas/order.xsd";

        Map<String, Object> parsed = Map.of("id", "X");
        XmlTypeClass<Object> xml = new FakeXmlPass(parsed);
        Object schema = new Object();
        Map<String, Object> nameToSchema = Map.of(schemaName, schema);
        RootConfig root = new RootConfig(null, schemaName);

        Parameters params = mock(Parameters.class);
        ParameterExtractor paramEx = new FakeParamExtractor(ErrorsOr.lift(params));

        IEventTypeExtractor eventTypeExtractor = message -> "SomeEvent";
        IDomainTypeExtractor domainTypeExtractor = message -> "orders";
        Map<String, Config> keyToConfig = Map.of(); // missing config

        InitialEnvelopeFactory<Object, String> sut =
                new InitialEnvelopeFactory<>(paramEx, nameToSchema, xml, eventTypeExtractor, domainTypeExtractor, keyToConfig, root);

        RawMessage raw = mock(RawMessage.class);
        when(raw.rawValue()).thenReturn("<ok/>");

        Envelope<String, Map<String, Object>> env = sut.createEnvelopeHeaderAtStart(raw, domainId);

        assertTrue(env instanceof ErrorEnvelope, "Expected ErrorEnvelope due to missing config");

        @SuppressWarnings("unchecked")
        ErrorEnvelope<String, Map<String, Object>> ee = (ErrorEnvelope<String, Map<String, Object>>) env;

        ValueEnvelope<String, Map<String, Object>> inner = ee.valueEnvelope();
        assertEquals(Map.of(), inner.data(), "recover payload should be empty Map");
        assertNull(inner.header().eventType());
        assertSame(raw, inner.header().rawMessage());
    }

    @Test
    void nullEventType_recoversToErrorEnvelope_withEmptyMapPayload() {
        String domainId   = "D-NULL-EVT";
        String schemaName = "schemas/order.xsd";

        Map<String, Object> parsed = Map.of("z", 1);
        XmlTypeClass<Object> xml = new FakeXmlPass(parsed);
        Object schema = new Object();
        Map<String, Object> nameToSchema = Map.of(schemaName, schema);
        RootConfig root = new RootConfig(null, schemaName);

        Parameters params = mock(Parameters.class);
        ParameterExtractor paramEx = new FakeParamExtractor(ErrorsOr.lift(params));

        IEventTypeExtractor eventTypeExtractor = message -> null; // force error
        IDomainTypeExtractor domainTypeExtractor = message -> "orders";
        Map<String, Config> keyToConfig = Map.of();

        InitialEnvelopeFactory<Object, String> sut =
                new InitialEnvelopeFactory<>(paramEx, nameToSchema, xml, eventTypeExtractor, domainTypeExtractor, keyToConfig, root);

        RawMessage raw = mock(RawMessage.class);
        when(raw.rawValue()).thenReturn("<ok/>");

        Envelope<String, Map<String, Object>> env = sut.createEnvelopeHeaderAtStart(raw, domainId);

        assertTrue(env instanceof ErrorEnvelope, "Expected ErrorEnvelope due to null event type");

        @SuppressWarnings("unchecked")
        ErrorEnvelope<String, Map<String, Object>> ee = (ErrorEnvelope<String, Map<String, Object>>) env;

        ValueEnvelope<String, Map<String, Object>> inner = ee.valueEnvelope();
        assertEquals(Map.of(), inner.data(), "recover payload should be empty Map");
        assertNull(inner.header().eventType());
        assertSame(raw, inner.header().rawMessage());
    }

    @Test
    void constructor_throwsWhenSchemaMissing() {
        String schemaName = "schemas/missing.xsd";
        RootConfig root = new RootConfig(null, schemaName);

        ParameterExtractor paramEx = new FakeParamExtractor(ErrorsOr.error("unused"));
        XmlTypeClass<Object> xml = new FakeXmlPass(Map.of());
        IEventTypeExtractor eventTypeExtractor = message -> "X";
        IDomainTypeExtractor domainTypeExtractor = message -> "orders";

        NullPointerException ex = assertThrows(NullPointerException.class, () ->
                new InitialEnvelopeFactory<>(
                        paramEx, Map.of(/* no schema */), xml, eventTypeExtractor, domainTypeExtractor, Map.of(), root
                ));
        assertTrue(ex.getMessage().contains("Schema not found"), "Error should mention missing schema");
    }
}
