package com.hcltech.rmg.messages;

import com.hcltech.rmg.cepstate.CepEvent;
import com.hcltech.rmg.cepstate.CepStateTypeClass;
import com.hcltech.rmg.common.errorsor.Value;
import com.hcltech.rmg.config.configs.Configs;
import com.hcltech.rmg.config.config.BehaviorConfig;
import com.hcltech.rmg.parameters.ParameterExtractor;
import com.hcltech.rmg.parameters.Parameters;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/**
 * Focused unit tests for Envelope family.
 *
 * Notes:
 *  - We keep generics simple (CepState = String, Msg = String) for readability.
 *  - External collaborators are mocked.
 */
class EnvelopeTest {

    @Test
    void valueEnvelope_withData_returnsNewInstanceWithUpdatedData() {
        var header = header("domainA", "evtA");
        var ve = new ValueEnvelope<String, String>(header, "old", "S0", new ArrayList<>());

        var ve2 = ve.withData("new");

        assertNotSame(ve, ve2);
        assertEquals("new", ve2.data());
        assertSame(header, ve2.header()); // unchanged
        assertEquals("S0", ve2.cepState()); // unchanged
        // cargo unchanged
        assertSame(header.cargo(), ve2.header().cargo());
    }

    @Test
    void valueEnvelope_map_appliesMapperFunction() {
        var header = header("domainA", "evtA");
        var ve = new ValueEnvelope<String, String>(header, "data", "S0", new ArrayList<>());

        Function<ValueEnvelope<String, String>, Envelope<String, String>> mapper =
                v -> new ErrorEnvelope<>(v, "stage-1", List.of("oops"));

        Envelope<String, String> out = ve.map(mapper);

        assertTrue(out instanceof ErrorEnvelope);
        assertSame(ve, ((ErrorEnvelope<String, String>) out).envelope());
    }

    @Test
    void interfaceDefault_map_onNonOverridingImplementations_isNoop() {
        // ErrorEnvelope/RetryEnvelope don't override map(Function)
        var header = header("domain", "evt");
        var value = new ValueEnvelope<String, String>(header, "data", "S", new ArrayList<>());

        Envelope<String, String> err = new ErrorEnvelope<>(value, "stage", List.of("e1"));
        Envelope<String, String> ret = new RetryEnvelope<>(value, "stage", 2);

        Envelope<String, String> errMapped = err.map(v -> new RetryEnvelope<>(v, "other", 0));
        Envelope<String, String> retMapped = ret.map(v -> new ErrorEnvelope<>(v, "other", List.of()));

        assertSame(err, errMapped);
        assertSame(ret, retMapped);
    }

    @Test
    void valueEnvelope_withNewCepEvent_updatesState_andRecordsEvent() {
        var header = header("domainA", "evtA");
        var events = new ArrayList<CepEvent>();
        var ve = new ValueEnvelope<String, String>(header, "data", "S0", events);

        @SuppressWarnings("unchecked")
        CepStateTypeClass<String> typeClass = mock(CepStateTypeClass.class);
        CepEvent event = mock(CepEvent.class);

        when(typeClass.processState("S0", event)).thenReturn("S1");

        var ve2 = ve.withNewCepEvent(typeClass, event);

        assertNotSame(ve, ve2);
        assertEquals("S1", ve2.cepState());
        assertEquals(1, ve2.cepStateModifications().size());
        assertSame(event, ve2.cepStateModifications().get(0));
        // cargo carried through (header instance is the same)
        assertSame(ve.header(), ve2.header());
        assertSame(ve.header().cargo(), ve2.header().cargo());
    }

    @Test
    void valueEnvelope_withNewCepEvent_nullEvent_isNoop() {
        var header = header("domainA", "evtA");
        var events = new ArrayList<CepEvent>();
        var ve = new ValueEnvelope<String, String>(header, "data", "S0", events);

        @SuppressWarnings("unchecked")
        CepStateTypeClass<String> typeClass = mock(CepStateTypeClass.class);

        var ve2 = ve.withNewCepEvent(typeClass, null);

        // Same instance when event is null (per implementation)
        assertSame(ve, ve2);
        assertEquals(0, ve2.cepStateModifications().size());
        verifyNoInteractions(typeClass);
    }

    @Test
    void envelope_keyForModule_delegatesToConfigsComposeKey() {
        var parameters = mock(Parameters.class);
        when(parameters.key()).thenReturn("PARAM_KEY");

        var header = new EnvelopeHeader<String>(
                "domainType",
                "EVENT_TYPE",
                mock(RawMessage.class),
                parameters,
                mock(BehaviorConfig.class),
                Map.of("foo", 42)
        );

        var ve = new ValueEnvelope<String, String>(header, "data", "S", new ArrayList<>());
        Envelope<String, String> env = ve;

        try (MockedStatic<Configs> cfg = mockStatic(Configs.class)) {
            cfg.when(() -> Configs.composeKey("PARAM_KEY", "EVENT_TYPE", "modA"))
                    .thenReturn("COMPOSED_KEY");

            String out = env.keyForModule("modA");
            assertEquals("COMPOSED_KEY", out);

            cfg.verify(() -> Configs.composeKey("PARAM_KEY", "EVENT_TYPE", "modA"));
        }
    }

    @Test
    void header_withMessage_setsEventTypeParameters_and_resetsCargoToEmpty() {
        var raw = mock(RawMessage.class);
        when(raw.domainId()).thenReturn("dom-1");

        // Start with some cargo to verify it is reset by withMessage(...)
        var original = new EnvelopeHeader<String>(
                "customer",
                null,         // eventType initially unknown
                raw,
                null,         // parameters initially unknown
                null,         // config initially unknown
                Map.of("testTag", "keep-alive")
        );

        @SuppressWarnings("unchecked")
        ParameterExtractor<String> extractor = mock(ParameterExtractor.class);
        var params = mock(Parameters.class);

        @SuppressWarnings("unchecked")
        Value<Parameters> value = (Value<Parameters>) mock(Value.class);

        when(extractor.parameters(eq("message-body"), eq("evtX"), eq("customer"), eq("dom-1")))
                .thenReturn(value);
        when(value.valueOrThrow()).thenReturn(params);

        var updated = original.<String>withMessage(extractor, "message-body", "evtX");

        assertEquals("customer", updated.domainType());
        assertEquals("evtX", updated.eventType());
        assertSame(raw, updated.rawMessage());
        assertSame(params, updated.parameters());
        // config preserved
        assertNull(updated.config());
        // cargo is explicitly reset to empty by withMessage(...)
        assertNotNull(updated.cargo());
        assertTrue(updated.cargo().isEmpty(), "withMessage should reset cargo to an empty map");
    }

    @Test
    void cargo_is_available_on_header_and_survives_valueEnvelope_lifecycle() {
        Map<String,Object> cargo = Map.of("expecter", "E-123", "trace", 99);
        var header = new EnvelopeHeader<String>(
                "domainA",
                "evtA",
                mock(RawMessage.class),
                mock(Parameters.class),
                mock(BehaviorConfig.class),
                cargo
        );

        var ve = new ValueEnvelope<String, String>(header, "data", "S0", new ArrayList<>());
        assertSame(cargo, ve.header().cargo());
        assertEquals("E-123", ve.header().cargo().get("expecter"));
        assertEquals(99, ve.header().cargo().get("trace"));

        // withData creates a new ValueEnvelope but preserves the header reference
        var ve2 = ve.withData("new-data");
        assertSame(ve.header(), ve2.header());
        assertSame(cargo, ve2.header().cargo());
    }

    @Test
    void equals_and_hashCode_basicContract() {
        var sharedHeader = header("d", "e"); // same instance

        var a = new ValueEnvelope<String, String>(sharedHeader, "X", "S", new ArrayList<>());
        var b = new ValueEnvelope<String, String>(sharedHeader, "X", "S", new ArrayList<>());

        assertEquals(a, b);
        assertEquals(a.hashCode(), b.hashCode());

        var c = a.withData("Y");
        assertNotEquals(a, c);
    }

    @Test
    void retryEnvelope_and_errorEnvelope_valueEnvelope_exposesInner() {
        var ve = new ValueEnvelope<String, String>(header("d","e"), "data", "S", new ArrayList<>());
        var err = new ErrorEnvelope<>(ve, "stage", List.of("err"));
        var retry = new RetryEnvelope<>(ve, "stage", 3);

        assertSame(ve, err.valueEnvelope());
        assertSame(ve, retry.valueEnvelope());
    }

    // ---- helpers ----

    private static EnvelopeHeader<String> header(String domain, String event) {
        var params = mock(Parameters.class);
        when(params.key()).thenReturn("PKEY");
        return new EnvelopeHeader<>(
                domain,
                event,
                mock(RawMessage.class),
                params,
                mock(BehaviorConfig.class),
                Map.of("seed", 1)
        );
    }
}
