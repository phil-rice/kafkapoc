// src/test/java/com/hcltech/rmg/messages/InitialEnvelopeFactoryTest.java
package com.hcltech.rmg.messages;

import com.hcltech.rmg.cepstate.CepEventLog;
import com.hcltech.rmg.cepstate.CepStateTypeClass;
import com.hcltech.rmg.cepstate.MapStringObjectCepStateTypeClass;
import com.hcltech.rmg.common.errorsor.ErrorsOr;
import com.hcltech.rmg.config.config.Config;
import com.hcltech.rmg.config.config.RootConfig;
import com.hcltech.rmg.parameters.ParameterExtractor;
import com.hcltech.rmg.parameters.Parameters;
import com.hcltech.rmg.xml.XmlTypeClass;
import org.junit.jupiter.api.Test;

import java.io.InputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/**
 * Tests InitialEnvelopeFactory with a mocked CepEventLog (safeFoldAll),
 * tiny fakes for XmlTypeClass + ParameterExtractor, and Mockito for
 * Parameters, Config, and RawMessage.
 */
class InitialEnvelopeFactoryTest {

    // Cep state typeclass we use across tests
    private static final CepStateTypeClass<Map<String, Object>> TC = new MapStringObjectCepStateTypeClass();

    // ---- Tiny fakes ---------------------------------------------------------

    /**
     * Fake XmlTypeClass that always succeeds with a given parsed map.
     */
    private static final class FakeXmlPass implements XmlTypeClass<Map<String, Object>, Object> {
        private final Map<String, Object> parsed;

        FakeXmlPass(Map<String, Object> parsed) {
            this.parsed = parsed;
        }

        @Override
        public Object loadSchema(String schemaName, InputStream schemaStream) {
            return new Object();
        }

        @Override
        public ErrorsOr<Map<String, Object>> parseAndValidate(String xml, Object schema) {
            return ErrorsOr.lift(parsed);
        }

        @Override
        public ErrorsOr<String> extractId(String rawString, List<String> idPath) {
            return ErrorsOr.error("unused");
        }
    }

    /**
     * Fake XmlTypeClass that always fails with a given error.
     */
    private static final class FakeXmlFail implements XmlTypeClass<Map<String, Object>, Object> {
        private final String err;

        FakeXmlFail(String err) {
            this.err = err;
        }

        @Override
        public Object loadSchema(String schemaName, InputStream schemaStream) {
            return new Object();
        }

        @Override
        public ErrorsOr<Map<String, Object>> parseAndValidate(String xml, Object schema) {
            return ErrorsOr.error(err);
        }

        @Override
        public ErrorsOr<String> extractId(String rawString, List<String> idPath) {
            return ErrorsOr.error("unused");
        }
    }

    /**
     * Fake ParameterExtractor that returns a pre-made Parameters value/errors.
     */
    private static final class FakeParamExtractor<Msg> implements ParameterExtractor<Msg> {
        private final ErrorsOr<Parameters> result;

        FakeParamExtractor(ErrorsOr<Parameters> result) {
            this.result = result;
        }

        @Override
        public ErrorsOr<Parameters> parameters(Msg input, String eventType, String domainType, String domainId) {
            return result;
        }
    }

    // ---- Helpers ------------------------------------------------------------

    private Supplier<CepEventLog> cepSupplierReturning(CepEventLog log) {
        return () -> log;
    }

    // ---- Tests --------------------------------------------------------------

    @Test
    void happyPath_buildsValueEnvelope_withEmptyCepState() {
        String domainId = "D-123";
        String schemaName = "schemas/order.xsd";

        Map<String, Object> parsed = Map.of("id", "ABC", "amount", 10);
        XmlTypeClass<Map<String, Object>, Object> xml = new FakeXmlPass(parsed);

        Object schema = new Object();
        Map<String, Object> nameToSchema = Map.of(schemaName, schema);
        RootConfig root = new RootConfig(null, schemaName);

        Parameters params = mock(Parameters.class);
        when(params.key()).thenReturn("dev:uk");
        ParameterExtractor<Map<String, Object>> paramEx = new FakeParamExtractor<>(ErrorsOr.lift(params));

        IEventTypeExtractor<Map<String, Object>> eventTypeExtractor = message -> "OrderCreated";
        IDomainTypeExtractor<Map<String, Object>> domainTypeExtractor = message -> "orders";

        Config cfg = mock(Config.class); // behaviorConfig() not asserted
        Map<String, Config> keyToConfig = Map.of("dev:uk", cfg);

        // CepEventLog that folds to empty map
        CepEventLog cepLog = mock(CepEventLog.class);
        when(cepLog.safeFoldAll(eq(TC), anyMap()))
                .thenAnswer(inv -> ErrorsOr.lift(new HashMap<>()));
        Supplier<CepEventLog> cepSupplier = cepSupplierReturning(cepLog);

        InitialEnvelopeFactory<Map<String, Object>, Map<String, Object>, Object> sut =
                new InitialEnvelopeFactory<>(paramEx, TC, nameToSchema, cepSupplier, xml, eventTypeExtractor, domainTypeExtractor, keyToConfig, root);

        RawMessage raw = mock(RawMessage.class);
        when(raw.rawValue()).thenReturn("<Order><Id>ABC</Id></Order>");

        Envelope<Map<String, Object>, Map<String, Object>> env = sut.createEnvelopeHeaderAtStart(raw, domainId);

        assertTrue(env instanceof ValueEnvelope, "Expected ValueEnvelope");

        @SuppressWarnings("unchecked")
        ValueEnvelope<Map<String, Object>, Map<String, Object>> ve = (ValueEnvelope<Map<String, Object>, Map<String, Object>>) env;

        // payload
        assertEquals(parsed, ve.data());

        // header fields
        EnvelopeHeader<Map<String, Object>> h = ve.header();
        assertEquals("orders", h.domainType());
        assertEquals(domainId, h.domainId());
        assertEquals("OrderCreated", h.eventType());
        assertSame(raw, h.rawMessage());
        assertSame(params, h.parameters());

        // cep state folded empty
        assertNotNull(h.cepState());
        assertTrue(h.cepState().isEmpty(), "cepState should be empty");
        verify(cepLog).safeFoldAll(eq(TC), anyMap());
    }

    @Test
    void happyPath_buildsValueEnvelope_withPrepopulatedCepState() {
        String domainId = "D-CEP";
        String schemaName = "schemas/order.xsd";

        Map<String, Object> parsed = Map.of("id", "ABC");
        XmlTypeClass<Map<String, Object>, Object> xml = new FakeXmlPass(parsed);

        Object schema = new Object();
        Map<String, Object> nameToSchema = Map.of(schemaName, schema);
        RootConfig root = new RootConfig(null, schemaName);

        Parameters params = mock(Parameters.class);
        when(params.key()).thenReturn("dev:uk");
        ParameterExtractor<Map<String, Object>> paramEx = new FakeParamExtractor<>(ErrorsOr.lift(params));

        IEventTypeExtractor<Map<String, Object>> eventTypeExtractor = message -> "OrderUpdated";
        IDomainTypeExtractor<Map<String, Object>> domainTypeExtractor = message -> "orders";

        Config cfg = mock(Config.class);
        Map<String, Config> keyToConfig = Map.of("dev:uk", cfg);

        // CepEventLog that folds to a pre-populated map
        Map<String, Object> folded = new HashMap<>();
        folded.put("lastEventType", "OrderCreated");
        folded.put("count", 7);

        CepEventLog cepLog = mock(CepEventLog.class);
        when(cepLog.safeFoldAll(eq(TC), anyMap())).thenReturn(ErrorsOr.lift(folded));
        Supplier<CepEventLog> cepSupplier = cepSupplierReturning(cepLog);

        InitialEnvelopeFactory<Map<String, Object>, Map<String, Object>, Object> sut =
                new InitialEnvelopeFactory<>(paramEx, TC, nameToSchema, cepSupplier, xml, eventTypeExtractor, domainTypeExtractor, keyToConfig, root);

        RawMessage raw = mock(RawMessage.class);
        when(raw.rawValue()).thenReturn("<Order/>");

        Envelope<Map<String, Object>, Map<String, Object>> env = sut.createEnvelopeHeaderAtStart(raw, domainId);

        assertTrue(env instanceof ValueEnvelope, "Expected ValueEnvelope");

        @SuppressWarnings("unchecked")
        ValueEnvelope<Map<String, Object>, Map<String, Object>> ve = (ValueEnvelope<Map<String, Object>, Map<String, Object>>) env;

        EnvelopeHeader<Map<String, Object>> h = ve.header();
        assertEquals("orders", h.domainType());
        assertEquals(domainId, h.domainId());
        assertEquals("OrderUpdated", h.eventType());
        assertSame(params, h.parameters());

        // cep state folded content present
        assertEquals(folded, h.cepState(), "cepState should be exactly the folded map");
        verify(cepLog).safeFoldAll(eq(TC), anyMap());
    }

    @Test
    void parseError_recoversToErrorEnvelope_withEmptyMapPayload() {
        String domainId = "D-ERR";
        String schemaName = "schemas/order.xsd";

        XmlTypeClass<Map<String, Object>, Object> xml = new FakeXmlFail("xml invalid");
        Object schema = new Object();
        Map<String, Object> nameToSchema = Map.of(schemaName, schema);
        RootConfig root = new RootConfig(null, schemaName);

        ParameterExtractor<Map<String, Object>> paramEx = new FakeParamExtractor<>(ErrorsOr.error("unused"));
        IEventTypeExtractor<Map<String, Object>> eventTypeExtractor = message -> "X";
        IDomainTypeExtractor<Map<String, Object>> domainTypeExtractor = message -> "orders";
        Map<String, Config> keyToConfig = Map.of();

        // CepEventLog (won't be called because parse fails)
        CepEventLog cepLog = mock(CepEventLog.class);
        Supplier<CepEventLog> cepSupplier = cepSupplierReturning(cepLog);

        InitialEnvelopeFactory<Map<String, Object>, Map<String, Object>, Object> sut =
                new InitialEnvelopeFactory<>(paramEx, TC, nameToSchema, cepSupplier, xml, eventTypeExtractor, domainTypeExtractor, keyToConfig, root);

        RawMessage raw = mock(RawMessage.class);
        when(raw.rawValue()).thenReturn("<broken/>");

        Envelope<Map<String, Object>, Map<String, Object>> env = sut.createEnvelopeHeaderAtStart(raw, domainId);

        assertTrue(env instanceof ErrorEnvelope, "Expected ErrorEnvelope");

        @SuppressWarnings("unchecked")
        ErrorEnvelope<Map<String, Object>, Map<String, Object>> ee = (ErrorEnvelope<Map<String, Object>, Map<String, Object>>) env;

        // inner value envelope crafted in recover(): header with null eventType, payload = null
        ValueEnvelope<Map<String, Object>, Map<String, Object>> inner = ee.valueEnvelope();
        EnvelopeHeader<Map<String, Object>> h = inner.header();
        assertNull(h.eventType(), "eventType should be null in error header");
        assertSame(raw, h.rawMessage());
        assertEquals(null, inner.data(), "recover payload should be null");

        verifyNoInteractions(cepLog);
    }

    @Test
    void missingConfig_recoversToErrorEnvelope_withEmptyMapPayload() {
        String domainId = "D-MISS-CFG";
        String schemaName = "schemas/order.xsd";

        Map<String, Object> parsed = Map.of("id", "X");
        XmlTypeClass<Map<String, Object>, Object> xml = new FakeXmlPass(parsed);
        Object schema = new Object();
        Map<String, Object> nameToSchema = Map.of(schemaName, schema);
        RootConfig root = new RootConfig(null, schemaName);

        Parameters params = mock(Parameters.class);
        when(params.key()).thenReturn("dev:uk");
        ParameterExtractor<Map<String, Object>> paramEx = new FakeParamExtractor<>(ErrorsOr.lift(params));

        IEventTypeExtractor<Map<String, Object>> eventTypeExtractor = message -> "SomeEvent";
        IDomainTypeExtractor<Map<String, Object>> domainTypeExtractor = message -> "orders";
        Map<String, Config> keyToConfig = Map.of(); // missing config

        // CepEventLog (will be invoked before missing-config check)
        CepEventLog cepLog = mock(CepEventLog.class);
        when(cepLog.safeFoldAll(eq(TC), anyMap())).thenReturn(ErrorsOr.lift(new HashMap<>()));
        Supplier<CepEventLog> cepSupplier = cepSupplierReturning(cepLog);

        InitialEnvelopeFactory<Map<String, Object>, Map<String, Object>, Object> sut =
                new InitialEnvelopeFactory<>(paramEx, TC, nameToSchema, cepSupplier, xml, eventTypeExtractor, domainTypeExtractor, keyToConfig, root);

        RawMessage raw = mock(RawMessage.class);
        when(raw.rawValue()).thenReturn("<ok/>");

        Envelope<Map<String, Object>, Map<String, Object>> env = sut.createEnvelopeHeaderAtStart(raw, domainId);

        assertTrue(env instanceof ErrorEnvelope, "Expected ErrorEnvelope due to missing config");

        @SuppressWarnings("unchecked")
        ErrorEnvelope<Map<String, Object>, Map<String, Object>> ee = (ErrorEnvelope<Map<String, Object>, Map<String, Object>>) env;

        ValueEnvelope<Map<String, Object>, Map<String, Object>> inner = ee.valueEnvelope();
        assertEquals(null, inner.data(), "recover payload should be null");
        assertNull(inner.header().eventType());
        assertSame(raw, inner.header().rawMessage());
        verify(cepLog).safeFoldAll(eq(TC), anyMap());
    }

    @Test
    void nullEventType_recoversToErrorEnvelope_withEmptyMapPayload() {
        String domainId = "D-NULL-EVT";
        String schemaName = "schemas/order.xsd";

        Map<String, Object> parsed = Map.of("z", 1);
        XmlTypeClass<Map<String, Object>, Object> xml = new FakeXmlPass(parsed);
        Object schema = new Object();
        Map<String, Object> nameToSchema = Map.of(schemaName, schema);
        RootConfig root = new RootConfig(null, schemaName);

        Parameters params = mock(Parameters.class);
        when(params.key()).thenReturn("dev:uk");
        ParameterExtractor<Map<String, Object>> paramEx = new FakeParamExtractor<>(ErrorsOr.lift(params));

        IEventTypeExtractor<Map<String, Object>> eventTypeExtractor = message -> null; // force error
        IDomainTypeExtractor<Map<String, Object>> domainTypeExtractor = message -> "orders";
        Map<String, Config> keyToConfig = Map.of();

        // CepEventLog should not be called because we short-circuit on null eventType
        CepEventLog cepLog = mock(CepEventLog.class);
        Supplier<CepEventLog> cepSupplier = cepSupplierReturning(cepLog);

        InitialEnvelopeFactory<Map<String, Object>, Map<String, Object>, Object> sut =
                new InitialEnvelopeFactory<>(paramEx, TC, nameToSchema, cepSupplier, xml, eventTypeExtractor, domainTypeExtractor, keyToConfig, root);

        RawMessage raw = mock(RawMessage.class);
        when(raw.rawValue()).thenReturn("<ok/>");

        Envelope<Map<String, Object>, Map<String, Object>> env = sut.createEnvelopeHeaderAtStart(raw, domainId);

        assertTrue(env instanceof ErrorEnvelope, "Expected ErrorEnvelope due to null event type");

        @SuppressWarnings("unchecked")
        ErrorEnvelope<Map<String, Object>, Map<String, Object>> ee = (ErrorEnvelope<Map<String, Object>, Map<String, Object>>) env;

        ValueEnvelope<Map<String, Object>, Map<String, Object>> inner = ee.valueEnvelope();
        assertEquals(null, inner.data(), "recover payload should be null");
        assertNull(inner.header().eventType());
        assertSame(raw, inner.header().rawMessage());

        verifyNoInteractions(cepLog);
    }

    @Test
    void constructor_throwsWhenSchemaMissing() {
        String schemaName = "schemas/missing.xsd";
        RootConfig root = new RootConfig(null, schemaName);

        ParameterExtractor<Map<String, Object>> paramEx = new FakeParamExtractor<>(ErrorsOr.error("unused"));
        XmlTypeClass<Map<String, Object>, Object> xml = new FakeXmlPass(Map.of());
        IEventTypeExtractor<Map<String, Object>> eventTypeExtractor = message -> "X";
        IDomainTypeExtractor<Map<String, Object>> domainTypeExtractor = message -> "orders";

        Supplier<CepEventLog> cepSupplier = () -> mock(CepEventLog.class);

        NullPointerException ex = assertThrows(NullPointerException.class, () ->
                new InitialEnvelopeFactory<>(
                        paramEx, TC, Map.of(/* no schema */), cepSupplier, xml, eventTypeExtractor, domainTypeExtractor, Map.of(), root
                ));
        assertTrue(ex.getMessage().contains("Schema not found"), "Error should mention missing schema");
    }
}
