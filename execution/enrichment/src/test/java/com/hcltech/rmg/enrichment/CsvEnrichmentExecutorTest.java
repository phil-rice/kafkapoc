package com.hcltech.rmg.enrichment;

import com.hcltech.rmg.cepstate.CepEvent;
import com.hcltech.rmg.cepstate.CepStateTypeClass;
import com.hcltech.rmg.cepstate.MapStringObjectCepStateTypeClass;
import com.hcltech.rmg.config.enrich.CsvEnrichment;
import com.hcltech.rmg.messages.EnvelopeHeader;
import com.hcltech.rmg.messages.MapStringObjectAndListStringMsgTypeClass;
import com.hcltech.rmg.messages.MsgTypeClass;
import com.hcltech.rmg.messages.RawMessage;
import com.hcltech.rmg.messages.ValueEnvelope;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * End-to-end tests for CsvEnrichmentExecutor with the finalized CsvEnrichment record.
 * Relies on CSV files under src/test/resources/CsvResourceLoaderTest/.
 *
 * sample.csv:
 *   a,b,X,Y,Z
 *   a1,b1,1,u,p
 *   a1,b2,2,v,q
 *   a2,b3,3
 */
class CsvEnrichmentExecutorTest {

    private static final String BASE = "CsvResourceLoaderTest/";

    private final MsgTypeClass<Map<String, Object>, List<String>> msgType =
            new MapStringObjectAndListStringMsgTypeClass();
    private final CepStateTypeClass<Map<String, Object>> cepType =
            new MapStringObjectCepStateTypeClass();

    private static ValueEnvelope<Map<String, Object>, Map<String, Object>> envWith(
            Map<String, Object> data,
            Map<String, Object> cepState
    ) {
        RawMessage raw = new RawMessage("{}", RawMessage.unknownDomainId, 0L, 0L, 0, 0, null, null, null);
        EnvelopeHeader<Map<String, Object>> header = new EnvelopeHeader<>(
                "domain", null, raw, null, null, null
        );
        return new ValueEnvelope<>(header, data, cepState, new ArrayList<>());
    }

    @BeforeEach
    void clearCache() {
        // ensure each test starts with a clean loader cache
        CsvEnrichmentExecutor.clearCache();
    }

    @Test
    void enriches_withDotDelimiter_pureInpInputs() {
        // inputs: take a and b from message payload
        CsvEnrichment cfg = new CsvEnrichment(
                List.of(List.of("message", "a"), List.of("message", "b")),
                List.of("enriched"),
                BASE + "sample.csv",
                List.of("a", "b"),
                List.of("Z", "X"),
                "."
        );

        Map<String, Object> msg = Map.of("a", "a1", "b", "b2");
        Map<String, Object> state = Map.of(); // not used
        ValueEnvelope<Map<String, Object>, Map<String, Object>> env = envWith(msg, state);

        CsvEnrichmentExecutor<Map<String, Object>, Map<String, Object>> exec =
                new CsvEnrichmentExecutor<>(cepType, msgType);

        CepEvent out = exec.execute("k", cfg, env);
        assertNotNull(out, "Expected an enrichment event");

        // Expect payload {Z: q, X: 2} at path ["enriched"]
        CepEvent expected = CepEvent.set(List.of("enriched"), Map.of("Z", "q", "X", "2"));
        assertEquals(expected, out);
    }

    @Test
    void enriches_withMixed_CepAndInpInputs() {
        // inputs: 'a' from CEP state, 'b' from message payload
        CsvEnrichment cfg = new CsvEnrichment(
                List.of(List.of("cepState", "a"), List.of("message", "b")),
                List.of("enriched"),
                BASE + "sample.csv",
                List.of("a", "b"),
                List.of("Z", "X"),
                "."
        );

        Map<String, Object> msg = Map.of("b", "b2");                  // from message
        Map<String, Object> state = Map.of("a", "a1");                 // from CEP state
        ValueEnvelope<Map<String, Object>, Map<String, Object>> env = envWith(msg, state);

        CsvEnrichmentExecutor<Map<String, Object>, Map<String, Object>> exec =
                new CsvEnrichmentExecutor<>(cepType, msgType);

        CepEvent out = exec.execute("k", cfg, env);
        assertNotNull(out);

        CepEvent expected = CepEvent.set(List.of("enriched"), Map.of("Z", "q", "X", "2"));
        assertEquals(expected, out);
    }

    @Test
    void enriches_withDashDelimiter_pureCepInputs() {
        // inputs: both from CEP state; delimiter is '-'
        CsvEnrichment cfg = new CsvEnrichment(
                List.of(List.of("cepState", "a"), List.of("cepState", "b")),
                List.of("enriched"),
                BASE + "sample.csv",
                List.of("a", "b"),
                List.of("Z", "X"),
                "-" // different delimiter
        );

        Map<String, Object> state = Map.of("a", "a1", "b", "b2");
        ValueEnvelope<Map<String, Object>, Map<String, Object>> env = envWith(Map.of(), state);

        CsvEnrichmentExecutor<Map<String, Object>, Map<String, Object>> exec =
                new CsvEnrichmentExecutor<>(cepType, msgType);

        CepEvent out = exec.execute("k", cfg, env);
        assertNotNull(out, "Expected enrichment even with non-dot delimiter");

        CepEvent expected = CepEvent.set(List.of("enriched"), Map.of("Z", "q", "X", "2"));
        assertEquals(expected, out);
    }

    @Test
    void returnsNull_whenRequiredInpMissing() {
        CsvEnrichment cfg = new CsvEnrichment(
                List.of(List.of("message", "a"), List.of("message", "b")),
                List.of("enriched"),
                BASE + "sample.csv",
                List.of("a", "b"),
                List.of("X", "Y"),
                "."
        );

        // Missing 'b'
        Map<String, Object> msg = Map.of("a", "a1");
        ValueEnvelope<Map<String, Object>, Map<String, Object>> env = envWith(msg, Map.of());

        CsvEnrichmentExecutor<Map<String, Object>, Map<String, Object>> exec =
                new CsvEnrichmentExecutor<>(cepType, msgType);

        assertNull(exec.execute("k", cfg, env), "No event when a required input component is missing");
    }

    @Test
    void returnsNull_whenCepRequestedButStateIsNull() {
        CsvEnrichment cfg = new CsvEnrichment(
                List.of(List.of("cepState", "a"), List.of("message", "b")),
                List.of("enriched"),
                BASE + "sample.csv",
                List.of("a", "b"),
                List.of("X", "Y"),
                "."
        );

        Map<String, Object> msg = Map.of("b", "b1");
        ValueEnvelope<Map<String, Object>, Map<String, Object>> env = envWith(msg, null); // no state

        CsvEnrichmentExecutor<Map<String, Object>, Map<String, Object>> exec =
                new CsvEnrichmentExecutor<>(cepType, msgType);

        assertNull(exec.execute("k", cfg, env), "No event when CEP path is requested but state is null");
    }

    @Test
    void returnsNull_whenNoMatchingRowInCsv() {
        CsvEnrichment cfg = new CsvEnrichment(
                List.of(List.of("message", "a"), List.of("message", "b")),
                List.of("enriched"),
                BASE + "sample.csv",
                List.of("a", "b"),
                List.of("X"),
                "."
        );

        // a/b combination not present in CSV
        Map<String, Object> msg = Map.of("a", "nope", "b", "b9");
        ValueEnvelope<Map<String, Object>, Map<String, Object>> env = envWith(msg, Map.of());

        CsvEnrichmentExecutor<Map<String, Object>, Map<String, Object>> exec =
                new CsvEnrichmentExecutor<>(cepType, msgType);

        assertNull(exec.execute("k", cfg, env), "No event when composite key not found in lookup");
    }

    @Test
    void returnsNull_whenNoCsvResourceConfigured() {
        // No csvFileName => executor has nothing to load; by design it returns null.
        CsvEnrichment cfg = new CsvEnrichment(
                List.of(List.of("message", "a"), List.of("message", "b")),
                List.of("enriched"),
                null,               // no resource
                List.of("a", "b"),
                List.of("X"),
                "."
        );

        Map<String, Object> msg = Map.of("a", "a1", "b", "b1");
        ValueEnvelope<Map<String, Object>, Map<String, Object>> env = envWith(msg, Map.of());

        CsvEnrichmentExecutor<Map<String, Object>, Map<String, Object>> exec =
                new CsvEnrichmentExecutor<>(cepType, msgType);

        assertNull(exec.execute("k", cfg, env),
                "With no csvFileName and no preloaded source, executor should return null");
    }

    @Test
    void repeatedCalls_useCachedLookupForSameConfigRecord() {
        CsvEnrichment cfg = new CsvEnrichment(
                List.of(List.of("message", "a"), List.of("message", "b")),
                List.of("enriched"),
                BASE + "sample.csv",
                List.of("a", "b"),
                List.of("Z", "X"),
                "."
        );

        Map<String, Object> msg = Map.of("a", "a1", "b", "b2");
        ValueEnvelope<Map<String, Object>, Map<String, Object>> env = envWith(msg, Map.of());

        CsvEnrichmentExecutor<Map<String, Object>, Map<String, Object>> exec =
                new CsvEnrichmentExecutor<>(cepType, msgType);

        // First call triggers lazy load + cache
        CepEvent first = exec.execute("k1", cfg, env);
        // Second call should hit the cache
        CepEvent second = exec.execute("k2", cfg, env);

        CepEvent expected = CepEvent.set(List.of("enriched"), Map.of("Z", "q", "X", "2"));
        assertEquals(expected, first);
        assertEquals(expected, second);
    }
}
