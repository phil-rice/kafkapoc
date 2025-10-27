package com.hcltech.rmg.enrichment;

import com.hcltech.rmg.cepstate.*;
import com.hcltech.rmg.common.apiclient.ApiClient;
import com.hcltech.rmg.config.enrich.ApiEnrichment;
import com.hcltech.rmg.messages.*;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.*;

import static java.util.Arrays.asList;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class ApiEnrichmentExecutorTest {

    private final CepStateTypeClass<Map<String, Object>> stateTc = new MapStringObjectCepStateTypeClass();
    private final MsgTypeClass<Map<String, Object>, List<String>> msgTc =
            new MapStringObjectAndListStringMsgTypeClass();

    private static <T> List<T> list(T... xs) { return asList(xs); }

    private static RawMessage makeRaw(String corrId) {
        return new RawMessage("raw", "dom", 1L, 1L, 0, 0, corrId, null, null);
    }

    private static ValueEnvelope<Map<String,Object>, Map<String,Object>> makeEnvelope(
            Map<String,Object> msg, Map<String,Object> cep, String corrId
    ) {
        EnvelopeHeader<Map<String, Object>> header =
                new EnvelopeHeader<>("domType", "evtType", makeRaw(corrId), null, null, Map.of());
        return new ValueEnvelope<>(header, msg, cep, new ArrayList<>());
    }

    // ---------------------------------------------------------------------

    @Nested @DisplayName("Constructor")
    class Ctor {
        @Test
        void requiresNonNullDeps() {
            @SuppressWarnings("unchecked")
            ApiClient<String, Map<String, Object>> api = mock(ApiClient.class);

            NullPointerException npe1 = assertThrows(NullPointerException.class,
                    () -> new ApiEnrichmentExecutor<>(null, stateTc, msgTc));
            assertEquals("apiClient", npe1.getMessage());

            NullPointerException npe2 = assertThrows(NullPointerException.class,
                    () -> new ApiEnrichmentExecutor<>(api, null, msgTc));
            assertEquals("cepStateTypeClass", npe2.getMessage());

            NullPointerException npe3 = assertThrows(NullPointerException.class,
                    () -> new ApiEnrichmentExecutor<>(api, stateTc, null));
            assertEquals("msgTypeClass", npe3.getMessage());

            assertDoesNotThrow(() -> new ApiEnrichmentExecutor<>(api, stateTc, msgTc));
        }
    }

    @Nested @DisplayName("execute")
    class Execute {

        @Test
        void throwsOnNullCfgOrEnvelope() {
            @SuppressWarnings("unchecked")
            ApiClient<String, Map<String, Object>> api = mock(ApiClient.class);
            var exec = new ApiEnrichmentExecutor<>(api, stateTc, msgTc);

            var cfg = new ApiEnrichment(List.of(list("a")), List.of("out"), "http://x", "q", 1000, 1000);
            var env = makeEnvelope(Map.of(), Map.of(), "corr-1");

            NullPointerException npe1 = assertThrows(NullPointerException.class,
                    () -> exec.execute("k", null, env));
            assertEquals("cfg", npe1.getMessage());

            NullPointerException npe2 = assertThrows(NullPointerException.class,
                    () -> exec.execute("k", cfg, null));
            assertEquals("envelope", npe2.getMessage());
        }

        @Test
        void missingInput_meansNoApiCall_andNullReturned() {
            @SuppressWarnings("unchecked")
            ApiClient<String, Map<String, Object>> api = mock(ApiClient.class);
            var exec = new ApiEnrichmentExecutor<>(api, stateTc, msgTc);

            var cfg = new ApiEnrichment(
                    List.of(list("customerId"), list("regionCode")), // regionCode missing
                    List.of("state", "enriched"),
                    "https://svc/enrich",
                    "q",
                    1_000, 1_000
            );

            Map<String, Object> msg = new HashMap<>();
            msg.put("customerId", "CUST-1");
            var env = makeEnvelope(msg, new HashMap<>(), "corr-1");

            assertNull(exec.execute("key", cfg, env));
            verifyNoInteractions(api);
        }

        @Test
        void apiReturnsNull_yieldsNoEvent_andPassesCorrId() {
            @SuppressWarnings("unchecked")
            ApiClient<String, Map<String, Object>> api = mock(ApiClient.class);
            var exec = new ApiEnrichmentExecutor<>(api, stateTc, msgTc);

            var cfg = new ApiEnrichment(
                    List.of(list("customerId"), list("regionCode")),
                    List.of("state", "enriched"),
                    "https://svc/enrich",
                    "q",
                    1_000, 1_000
            );

            Map<String, Object> msg = Map.of("customerId", "CUST-2", "regionCode", "EU");
            var env = makeEnvelope(msg, new HashMap<>(), "trace-xyz");

            when(api.fetch("https://svc/enrich", "q", "trace-xyz", "CUST-2,EU")).thenReturn(null);

            assertNull(exec.execute("k", cfg, env));
            verify(api).fetch("https://svc/enrich", "q", "trace-xyz", "CUST-2,EU");
            verifyNoMoreInteractions(api);
        }

        @Test
        void happyPath_emitsCepSetEvent_processStateWritesResult_andPassesCorrId() {
            @SuppressWarnings("unchecked")
            ApiClient<String, Map<String, Object>> api = mock(ApiClient.class);
            var exec = new ApiEnrichmentExecutor<>(api, stateTc, msgTc);

            List<String> outPath = list("state", "enriched");
            var cfg = new ApiEnrichment(
                    List.of(list("customerId")),
                    outPath,
                    "https://svc/enrich",
                    "q",
                    1_000, 1_000
            );

            Map<String, Object> msg = Map.of("customerId", "CUST-3");
            Map<String, Object> cep = new HashMap<>();
            var env = makeEnvelope(msg, cep, "corr-777");

            Map<String, Object> apiResult = Map.of("foo", 42, "bar", "baz");
            when(api.fetch("https://svc/enrich", "q", "corr-777", "CUST-3")).thenReturn(apiResult);

            CepEvent evt = exec.execute("key", cfg, env);
            assertNotNull(evt, "Should emit an event");
            assertTrue(evt instanceof CepSetEvent, "Should be a CepSetEvent");

            // Apply event using real state type class and verify
            Map<String, Object> updated = stateTc.processState(cep, evt);
            Object written = stateTc.getFromPath(updated, outPath);
            assertSame(apiResult, written, "CEP state should contain the API result");

            verify(api).fetch("https://svc/enrich", "q", "corr-777", "CUST-3");
            verifyNoMoreInteractions(api);
        }
    }
}
