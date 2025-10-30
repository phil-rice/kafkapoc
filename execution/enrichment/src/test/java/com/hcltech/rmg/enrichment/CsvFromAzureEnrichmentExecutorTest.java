package com.hcltech.rmg.enrichment;

import com.hcltech.rmg.cepstate.CepEvent;
import com.hcltech.rmg.cepstate.CepStateTypeClass;
import com.hcltech.rmg.cepstate.MapStringObjectCepStateTypeClass;
import com.hcltech.rmg.common.azure_blob_storage.AzureBlobConfig;
import com.hcltech.rmg.common.tokens.ITokenGenerator;
import com.hcltech.rmg.common.tokens.Token;
import com.hcltech.rmg.config.enrich.CsvFromAzureEnrichment;
import com.hcltech.rmg.messages.MapStringObjectAndListStringMsgTypeClass;
import com.hcltech.rmg.messages.MsgTypeClass;
import com.hcltech.rmg.messages.ValueEnvelope;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class CsvFromAzureEnrichmentExecutorTest {

    /** Simple fixed token generator that always returns a SAS token. */
    private static final class FixedSasTokenGen implements ITokenGenerator {
        private final String sas; // should include or omit '?' — client will accept either
        private FixedSasTokenGen(String sas) { this.sas = sas; }
        @Override public Token token(String envVariableNameOrNull) {
            String normalized = sas.startsWith("?") ? sas : "?" + sas;
            return new Token(Token.Type.SAS, normalized);
        }
    }

    @AfterEach
    void tearDown() {
        CsvFromAzureEnrichmentExecutor.clearCache();
    }

    // Helper to avoid generic matcher pain on HttpClient.send(...)
    @SuppressWarnings("unchecked")
    private static HttpResponse.BodyHandler<InputStream> anyInputStreamHandler() {
        return (HttpResponse.BodyHandler<InputStream>) any(HttpResponse.BodyHandler.class);
    }

    @Test
    void execute_happyPath_usesRealHelpers_andCachesLookup() throws Exception {
        // CSV with key column 'code' and output column 'name'
        String csv = "code,name\nGB,United Kingdom\nUS,United States\n";
        byte[] csvBytes = csv.getBytes(StandardCharsets.UTF_8);

        // Azure config (base info; SAS is provided by token generator now)
        AzureBlobConfig az = new AzureBlobConfig(
                "acct", "data", "lookups/countries.csv",
                "LOCAL_BLOB_SAS",   // sasEnvVar name (not actually used by FixedSasTokenGen)
                null                // endpointHost
        );

        // Enrichment config
        CsvFromAzureEnrichment cfg = new CsvFromAzureEnrichment(
                List.of(List.of("message", "code")),
                List.of("enriched"),
                az,
                List.of("code"),
                List.of("name"),
                "."
        );

        // Real helper implementations
        MsgTypeClass<Map<String, Object>, List<String>> msgType = new MapStringObjectAndListStringMsgTypeClass();
        CepStateTypeClass<Map<String, Object>> cepType = new MapStringObjectCepStateTypeClass();

        // Mock HttpClient -> returns the CSV stream once
        HttpClient http = mock(HttpClient.class);
        @SuppressWarnings("unchecked")
        HttpResponse<InputStream> resp = (HttpResponse<InputStream>) mock(HttpResponse.class);
        when(resp.statusCode()).thenReturn(200);
        when(resp.body()).thenReturn(new ByteArrayInputStream(csvBytes));
        when(http.send(any(HttpRequest.class), anyInputStreamHandler())).thenReturn(resp);

        // Mock ValueEnvelope
        @SuppressWarnings("unchecked")
        ValueEnvelope<Map<String, Object>, Map<String, Object>> env =
                (ValueEnvelope<Map<String, Object>, Map<String, Object>>) mock(ValueEnvelope.class);

        Map<String, Object> msgData = new HashMap<>();
        msgData.put("code", "GB");
        when(env.data()).thenReturn(msgData);
        when(env.cepState()).thenReturn(null);

        // Executor with injected HttpClient and SAS token generator
        ITokenGenerator tokenGen = new FixedSasTokenGen("?sv=1&sig=abc");
        CsvFromAzureEnrichmentExecutor<Map<String, Object>, Map<String, Object>> exec =
                new CsvFromAzureEnrichmentExecutor<>(cepType, msgType, tokenGen, http);

        // First execute -> should load CSV over HTTP, build lookup, emit event
        CepEvent ev1 = exec.execute("k", cfg, env);
        assertNotNull(ev1, "Expected enrichment event on first execution");

        Map<String, Object> state1 = cepType.createEmpty();
        Map<String, Object> updated1 = cepType.processState(state1, ev1);
        @SuppressWarnings("unchecked")
        Map<String, Object> enriched1 = (Map<String, Object>) updated1.get("enriched");
        assertNotNull(enriched1, "Expected 'enriched' path to be set");
        assertEquals("United Kingdom", enriched1.get("name"));

        // Second execute with same cfg -> should use cache (no extra HTTP)
        CepEvent ev2 = exec.execute("k", cfg, env);
        assertNotNull(ev2, "Expected enrichment event on second execution");
        Map<String, Object> updated2 = cepType.processState(cepType.createEmpty(), ev2);
        @SuppressWarnings("unchecked")
        Map<String, Object> enriched2 = (Map<String, Object>) updated2.get("enriched");
        assertNotNull(enriched2);
        assertEquals("United Kingdom", enriched2.get("name"));

        // Verify only one HTTP call happened -> proves cache was used
        verify(http, times(1)).send(any(HttpRequest.class), anyInputStreamHandler());
        verifyNoMoreInteractions(http);
    }

    @Test
    void execute_returnsNull_whenNoAzureConfig() {
        CsvFromAzureEnrichment cfg = new CsvFromAzureEnrichment(
                List.of(List.of("inp", "code")),
                List.of("enriched"),
                null,                       // no Azure -> nothing to load
                List.of("code"),
                List.of("name"),
                "."
        );

        MsgTypeClass<Map<String, Object>, List<String>> msgType = new MapStringObjectAndListStringMsgTypeClass();
        CepStateTypeClass<Map<String, Object>> cepType = new MapStringObjectCepStateTypeClass();

        HttpClient http = mock(HttpClient.class);

        @SuppressWarnings("unchecked")
        ValueEnvelope<Map<String, Object>, Map<String, Object>> env =
                (ValueEnvelope<Map<String, Object>, Map<String, Object>>) mock(ValueEnvelope.class);
        when(env.data()).thenReturn(Map.of("code", "GB"));
        when(env.cepState()).thenReturn(null);

        ITokenGenerator tokenGen = new FixedSasTokenGen("?sv=1&sig=abc"); // unused here
        CsvFromAzureEnrichmentExecutor<Map<String, Object>, Map<String, Object>> exec =
                new CsvFromAzureEnrichmentExecutor<>(cepType, msgType, tokenGen, http);

        CepEvent ev = exec.execute("k", cfg, env);
        assertNull(ev, "No Azure config -> executor should produce no event");

        verifyNoInteractions(http);
    }

    @Test
    void execute_returnsNull_whenKeyNotFoundInLookup() throws Exception {
        // CSV has GB and US; we'll ask for FR
        String csv = "code,name\nGB,United Kingdom\nUS,United States\n";
        byte[] csvBytes = csv.getBytes(StandardCharsets.UTF_8);

        AzureBlobConfig az = new AzureBlobConfig(
                "acct", "data", "lookups/countries.csv",
                "LOCAL_BLOB_SAS", null);

        CsvFromAzureEnrichment cfg = new CsvFromAzureEnrichment(
                List.of(List.of("inp", "code")),
                List.of("enriched"),
                az,
                List.of("code"),
                List.of("name"),
                "."
        );

        MsgTypeClass<Map<String, Object>, List<String>> msgType = new MapStringObjectAndListStringMsgTypeClass();
        CepStateTypeClass<Map<String, Object>> cepType = new MapStringObjectCepStateTypeClass();

        HttpClient http = mock(HttpClient.class);
        @SuppressWarnings("unchecked")
        HttpResponse<InputStream> resp = (HttpResponse<InputStream>) mock(HttpResponse.class);
        when(resp.statusCode()).thenReturn(200);
        when(resp.body()).thenReturn(new ByteArrayInputStream(csvBytes));
        when(http.send(any(HttpRequest.class), anyInputStreamHandler())).thenReturn(resp);

        @SuppressWarnings("unchecked")
        ValueEnvelope<Map<String, Object>, Map<String, Object>> env =
                (ValueEnvelope<Map<String, Object>, Map<String, Object>>) mock(ValueEnvelope.class);
        when(env.data()).thenReturn(Map.of("code", "FR")); // not present in CSV
        when(env.cepState()).thenReturn(null);

        ITokenGenerator tokenGen = new FixedSasTokenGen("?sv=1&sig=abc");
        CsvFromAzureEnrichmentExecutor<Map<String, Object>, Map<String, Object>> exec =
                new CsvFromAzureEnrichmentExecutor<>(cepType, msgType, tokenGen, http);

        CepEvent ev = exec.execute("k", cfg, env);
        assertNull(ev, "Unknown key -> no enrichment event");

        verify(http, times(1)).send(any(HttpRequest.class), anyInputStreamHandler());
        verifyNoMoreInteractions(http);
    }

    @Test
    void execute_cepStateSourceAlsoWorks() throws Exception {
        // CSV with code->name
        String csv = "code,name\nGB,United Kingdom\n";
        byte[] csvBytes = csv.getBytes(StandardCharsets.UTF_8);

        AzureBlobConfig az = new AzureBlobConfig(
                "acct", "data", "lookups/countries.csv",
                "LOCAL_BLOB_SAS", null);

        CsvFromAzureEnrichment cfg = new CsvFromAzureEnrichment(
                List.of(List.of("cepState", "ctx", "code")),  // read from CEP state at ["ctx","code"]
                List.of("enriched"),
                az,
                List.of("code"),
                List.of("name"),
                "."
        );

        MsgTypeClass<Map<String, Object>, List<String>> msgType = new MapStringObjectAndListStringMsgTypeClass();
        CepStateTypeClass<Map<String, Object>> cepType = new MapStringObjectCepStateTypeClass();

        HttpClient http = mock(HttpClient.class);
        @SuppressWarnings("unchecked")
        HttpResponse<InputStream> resp = (HttpResponse<InputStream>) mock(HttpResponse.class);
        when(resp.statusCode()).thenReturn(200);
        when(resp.body()).thenReturn(new ByteArrayInputStream(csvBytes));
        when(http.send(any(HttpRequest.class), anyInputStreamHandler())).thenReturn(resp);

        // Envelope with CEP state containing ctx.code = "GB"
        Map<String, Object> cepState = new HashMap<>();
        cepState.put("ctx", Map.of("code", "GB"));

        @SuppressWarnings("unchecked")
        ValueEnvelope<Map<String, Object>, Map<String, Object>> env =
                (ValueEnvelope<Map<String, Object>, Map<String, Object>>) mock(ValueEnvelope.class);
        when(env.data()).thenReturn(Map.of()); // message not used
        when(env.cepState()).thenReturn(cepState);

        ITokenGenerator tokenGen = new FixedSasTokenGen("?sv=1&sig=abc");
        CsvFromAzureEnrichmentExecutor<Map<String, Object>, Map<String, Object>> exec =
                new CsvFromAzureEnrichmentExecutor<>(cepType, msgType, tokenGen, http);

        CepEvent ev = exec.execute("k", cfg, env);
        assertNotNull(ev, "Expected enrichment event when key comes from CEP state");

        Map<String, Object> target = cepType.processState(cepType.createEmpty(), ev);
        @SuppressWarnings("unchecked")
        Map<String, Object> enriched = (Map<String, Object>) target.get("enriched");
        assertEquals("United Kingdom", enriched.get("name"));
    }
}
