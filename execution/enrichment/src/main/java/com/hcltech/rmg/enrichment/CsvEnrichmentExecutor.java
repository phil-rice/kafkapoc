package com.hcltech.rmg.enrichment;

import com.hcltech.rmg.cepstate.CepEvent;
import com.hcltech.rmg.cepstate.CepStateTypeClass;
import com.hcltech.rmg.common.csv.CsvResourceLoader;
import com.hcltech.rmg.config.enrich.CsvEnrichment;
import com.hcltech.rmg.execution.aspects.AspectExecutor;
import com.hcltech.rmg.messages.MsgTypeClass;
import com.hcltech.rmg.messages.ValueEnvelope;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Executor for {@link CsvEnrichment} (config-only).
 *
 * Flow:
 *  1) Build a composite key from {@code cfg.inputs()} by reading values from either:
 *        - "inp":  ValueEnvelope.data() via {@link MsgTypeClass}
 *        - "cep":  ValueEnvelope.cepState() via {@link CepStateTypeClass}
 *     Join parts with {@code cfg.keyDelimiter()}.
 *  2) Resolve a cached lookup map for {@code cfg}:
 *        If {@code cfg.csvFileName()} is non-empty, lazily load once via {@link CsvResourceLoader}
 *        using the SAME key delimiter; cache is keyed by the {@code CsvEnrichment} record itself.
 *        If no resource, return {@code null} (nothing to enrich from).
 *  3) Lookup the composite key; if found, map the resulting List&lt;String&gt; to {@code cfg.outputColumns()},
 *     padding with {@code null} as needed.
 *  4) Emit {@code CepEvent.set(cfg.output(), payload)}; otherwise return {@code null}.
 *
 * @param <CepState> type of CEP state carried by {@link ValueEnvelope}
 * @param <Msg>      type of the message payload in {@link ValueEnvelope}
 */
public final class CsvEnrichmentExecutor<CepState, Msg>
        implements AspectExecutor<CsvEnrichment, ValueEnvelope<CepState, Msg>, CepEvent> {

    /** Cache of loaded CSV lookups keyed by the immutable {@link CsvEnrichment} record. */
    private static final ConcurrentHashMap<CsvEnrichment, Map<String, List<String>>> LOOKUP_CACHE =
            new ConcurrentHashMap<>();

    private static final String ROOT_INP = "inp";
    private static final String ROOT_CEP = "cep";

    private final MsgTypeClass<Msg, List<String>> msgTypeClass;
    private final CepStateTypeClass<CepState>     cepStateTypeClass;

    public CsvEnrichmentExecutor(CepStateTypeClass<CepState> cepStateTypeClass, MsgTypeClass<Msg, List<String>> msgTypeClass) {
        this.msgTypeClass = Objects.requireNonNull(msgTypeClass, "msgTypeClass");
        this.cepStateTypeClass = Objects.requireNonNull(cepStateTypeClass, "cepStateTypeClass");
    }

    /** For tests / lifecycle management: clears all cached lookups. */
    public static void clearCache() {
        LOOKUP_CACHE.clear();
    }

    @Override
    public CepEvent execute(String key, CsvEnrichment cfg, ValueEnvelope<CepState, Msg> input) {
        // 1) Resolve (possibly cached) lookup map for this exact config record
        final Map<String, List<String>> lookup = resolveLookup(cfg);
        if (lookup == null || lookup.isEmpty()) {
            return null; // nothing to enrich from
        }

        // 2) Build composite key from message/state using cfg.inputs() and cfg.keyDelimiter()
        final String compositeKey = buildCompositeKey(cfg, input);
        if (compositeKey == null) {
            return null; // missing required input -> no enrichment
        }

        // 3) Lookup values
        final List<String> values = lookup.get(compositeKey);
        if (values == null) {
            return null; // no matching row
        }

        // 4) Map values to output columns, padding with nulls for short rows
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

    private Map<String, List<String>> resolveLookup(CsvEnrichment cfg) {
        final String resource = cfg.csvFileName();
        if (resource == null || resource.isBlank()) {
            return null; // no resource -> nothing to load
        }
        // Cache keyed by the immutable config record itself (record's equals/hashCode suffice)
        return LOOKUP_CACHE.computeIfAbsent(
                cfg,
                c -> CsvResourceLoader
                        .load(c.csvFileName(), c.inputColumns(), c.outputColumns(), ',', c.keyDelimiter())
                        .map()
        );
    }

    private String buildCompositeKey(CsvEnrichment cfg, ValueEnvelope<CepState, Msg> input) {
        final StringBuilder sb = new StringBuilder();
        final String delim = cfg.keyDelimiter();

        for (List<String> rawPath : cfg.inputs()) {
            if (rawPath == null || rawPath.isEmpty()) {
                return null; // no root selector provided -> treat as missing
            }

            final String root = rawPath.get(0);
            final List<String> path = rawPath.size() > 1 ? rawPath.subList(1, rawPath.size()) : List.of();

            Object value;
            if (ROOT_INP.equals(root)) {
                // Read from message payload via MsgTypeClass
                value = msgTypeClass.getValueFromPath(input.data(), path);
            } else if (ROOT_CEP.equals(root)) {
                // Read from CEP state via CepStateTypeClass
                final CepState state = input.cepState();
                if (state == null) {
                    return null; // no state available but requested
                }
                value = cepStateTypeClass.getFromPath(state, path);
            } else {
                // Fallback to legacy behavior: treat whole path as data path
                // If you want to enforce "must be 'inp' or 'cep'", replace with: return null;
                value = msgTypeClass.getValueFromPath(input.data(), rawPath);
            }

            if (value == null) {
                return null; // missing component -> abort enrichment
            }

            if (sb.length() > 0) {
                sb.append(delim);
            }
            sb.append(String.valueOf(value));
        }

        return sb.toString();
    }
}
