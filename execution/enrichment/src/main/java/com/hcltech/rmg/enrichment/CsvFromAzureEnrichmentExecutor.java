package com.hcltech.rmg.enrichment;

import com.hcltech.rmg.cepstate.CepEvent;
import com.hcltech.rmg.cepstate.CepStateTypeClass;
import com.hcltech.rmg.common.azure_blob_storage.AzureBlobClient;
import com.hcltech.rmg.common.azure_blob_storage.AzureBlobConfig;
import com.hcltech.rmg.common.csv.CsvResourceLoader;
import com.hcltech.rmg.common.tokens.ITokenGenerator;
import com.hcltech.rmg.config.enrich.CsvFromAzureEnrichment;
import com.hcltech.rmg.execution.aspects.AspectExecutor;
import com.hcltech.rmg.messages.MsgTypeClass;
import com.hcltech.rmg.messages.ValueEnvelope;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.net.http.HttpClient;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Executor for {@link CsvFromAzureEnrichment}.
 *
 * Flow:
 *  1) Resolve a cached lookup map for {@code cfg}:
 *        If {@code cfg.azure()} is non-null, load once via {@link AzureBlobClient}
 *        and parse with {@link CsvResourceLoader#loadFromInputStream(InputStream, char, String, List, List, String)}.
 *        Cache is keyed by the {@code CsvFromAzureEnrichment} record itself.
 *        If no Azure config, return {@code null} (nothing to enrich from).
 *  2) Build a composite key from {@code cfg.inputs()} using {@link EnricherHelper#buildCompositeKey}.
 *  3) Lookup the composite key; if found, map the resulting List&lt;String&gt; to {@code cfg.outputColumns()},
 *     padding with {@code null} as needed.
 *  4) Emit {@code CepEvent.set(cfg.output(), payload)}; otherwise return {@code null}.
 *
 * @param <CepState> type of CEP state carried by {@link ValueEnvelope}
 * @param <Msg>      type of the message payload in {@link ValueEnvelope}
 */
public final class CsvFromAzureEnrichmentExecutor<CepState, Msg>
        implements AspectExecutor<CsvFromAzureEnrichment, ValueEnvelope<CepState, Msg>, CepEvent> {

    private static final Logger log = LoggerFactory.getLogger(CsvFromAzureEnrichmentExecutor.class);
    /** Cache of loaded CSV lookups keyed by the immutable {@link CsvFromAzureEnrichment} record. */
    private static final ConcurrentHashMap<CsvFromAzureEnrichment, Map<String, List<String>>> LOOKUP_CACHE =
            new ConcurrentHashMap<>();

    private final MsgTypeClass<Msg, List<String>> msgTypeClass;
    private final CepStateTypeClass<CepState>     cepStateTypeClass;
    private final HttpClient httpClient;
    private final ITokenGenerator tokenGenerator;

    /** DI-friendly ctor (provide your token generator; e.g., AzureStorageTokenGenerator). Uses default JDK HttpClient. */
    public CsvFromAzureEnrichmentExecutor(CepStateTypeClass<CepState> cepStateTypeClass,
                                          MsgTypeClass<Msg, List<String>> msgTypeClass,
                                          ITokenGenerator tokenGenerator) {
        this(cepStateTypeClass, msgTypeClass, tokenGenerator, HttpClient.newHttpClient());
    }

    /** Full-args ctor for tests (inject mocked/custom HttpClient and token generator). */
    public CsvFromAzureEnrichmentExecutor(CepStateTypeClass<CepState> cepStateTypeClass,
                                          MsgTypeClass<Msg, List<String>> msgTypeClass,
                                          ITokenGenerator tokenGenerator,
                                          HttpClient httpClient) {
        this.msgTypeClass = Objects.requireNonNull(msgTypeClass, "msgTypeClass");
        this.cepStateTypeClass = Objects.requireNonNull(cepStateTypeClass, "cepStateTypeClass");
        this.tokenGenerator = Objects.requireNonNull(tokenGenerator, "tokenGenerator");
        this.httpClient = Objects.requireNonNull(httpClient, "httpClient");
    }

    /** For tests / lifecycle management: clears all cached lookups. */
    public static void clearCache() {
        LOOKUP_CACHE.clear();
    }

    @Override
    public CepEvent execute(String key, CsvFromAzureEnrichment cfg, ValueEnvelope<CepState, Msg> input) {
        // 1) Resolve (possibly cached) lookup map for this exact config record
        final Map<String, List<String>> lookup = resolveLookup(cfg);
        if (lookup == null || lookup.isEmpty()) return null;

        // 2) Build composite key using helper
        final String compositeKey = EnricherHelper.buildCompositeKey(
                cfg.inputs(),
                cfg.keyDelimiter(),
                input,
                msgTypeClass,
                cepStateTypeClass
        );
        if (compositeKey == null) return null;

        // 3) Lookup values
        final List<String> values = lookup.get(compositeKey);
        if (values == null) return null;

        // 4) Map values to output columns (pad with nulls)
        final List<String> outCols = cfg.outputColumns();
        final Map<String, Object> payload = new HashMap<>(Math.max(4, outCols.size() * 2));
        for (int i = 0; i < outCols.size(); i++) {
            final String col = outCols.get(i);
            final String val = (i < values.size()) ? values.get(i) : null;
            payload.put(col, val);
        }

        // 5) Emit event to set the payload at the configured output path
        return CepEvent.set(cfg.output(), payload);
    }

    // -------------------- internals --------------------

    private Map<String, List<String>> resolveLookup(CsvFromAzureEnrichment cfg) {
        final AzureBlobConfig az = cfg.azure();
        if (az == null) return null; // nothing to load

        // Cache keyed by the immutable config record itself
        return LOOKUP_CACHE.computeIfAbsent(cfg, c -> {
            final String desc = safeDescribe(az);
            log.info("Loading azure desc = " + desc);
            try (InputStream is = AzureBlobClient.openBlobStream(az, tokenGenerator, httpClient)) {
                // ',' CSV delimiter; composite key delimiter from cfg
                return CsvResourceLoader
                        .loadFromInputStream(is, ',', desc, c.inputColumns(), c.outputColumns(), c.keyDelimiter())
                        .map();
            } catch (Exception e) {
                log.error("Error while loading Azure blob stream", e);
                throw new RuntimeException("Error loading CSV from Azure blob: " + desc, e);
            }
        });
    }

    private static String safeDescribe(AzureBlobConfig az) {
        try {
            return az.blobUri().toString();
        } catch (Exception ignore) {
            return "azure://" + az.accountName() + "/" + az.container() + "/" + az.blobPath();
        }
    }
}
