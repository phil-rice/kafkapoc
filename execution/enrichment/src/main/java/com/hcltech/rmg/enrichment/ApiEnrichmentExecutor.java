package com.hcltech.rmg.enrichment;

import com.hcltech.rmg.cepstate.CepEvent;
import com.hcltech.rmg.cepstate.CepStateTypeClass;
import com.hcltech.rmg.common.apiclient.ApiClient;
import com.hcltech.rmg.config.enrich.ApiEnrichment;
import com.hcltech.rmg.execution.aspects.AspectExecutor;
import com.hcltech.rmg.messages.MsgTypeClass;
import com.hcltech.rmg.messages.ValueEnvelope;

import java.util.List;
import java.util.Objects;

/**
 * Executes {@link ApiEnrichment}:
 * 1) Build a composite input string from the ValueEnvelope using EnricherHelper and the configured input paths.
 * 2) Call the API client with that string (your client can interpret param name / url config).
 * 3) If non-null, emit a CepEvent to write results under the configured output path(s).
 * <p>
 * Notes:
 * - ValueEnvelope contains all run context; CEP state may be a Map<String,Object>.
 * - The exact fan-out for multiple output paths can be decided once API response shape is fixed.
 */
public final class ApiEnrichmentExecutor<CepState, Msg>
        implements AspectExecutor<ApiEnrichment, ValueEnvelope<CepState, Msg>, CepEvent> {

    private final ApiClient<String, CepState> apiClient;
    private final CepStateTypeClass<CepState> cepStateTypeClass;
    private final MsgTypeClass<Msg, List<String>> msgTypeClass;

    public ApiEnrichmentExecutor(ApiClient<String, CepState> apiClient,
                                 CepStateTypeClass<CepState> cepStateTypeClass,
                                 MsgTypeClass<Msg, List<String>> msgTypeClass) {
        this.apiClient = Objects.requireNonNull(apiClient, "apiClient");
        this.cepStateTypeClass = Objects.requireNonNull(cepStateTypeClass, "cepStateTypeClass");
        this.msgTypeClass = Objects.requireNonNull(msgTypeClass, "msgTypeClass");
    }

    @Override
    public CepEvent execute(String key, ApiEnrichment cfg, ValueEnvelope<CepState, Msg> envelope) {
        Objects.requireNonNull(cfg, "cfg");
        Objects.requireNonNull(envelope, "envelope");

        // 1) Build the composite parameter (returns null if any component missing)
        final String param =
                EnricherHelper.buildCompositeKey(cfg.inputs(), ",", envelope, msgTypeClass, cepStateTypeClass);

        if (param == null || param.isBlank()) {
            // Missing inputs or all-null/blank after join → nothing to enrich
            return null;
        }

        // 2) Call API.
        String corrId = envelope.header().rawMessage().traceparent();
        final CepState result = apiClient.fetch(cfg.url(), cfg.paramName(), corrId, param);
        if (result == null) {
            // No data → skip write
            return null;
        }

        // 3) Emit event to write into CEP state.
        //    cfg.output() is a List<String> path(s). If you later decide to fan out multiple fields,
        //    you can adapt here based on `result` shape (Map/List/etc.).
        return CepEvent.set(cfg.output(), result);
    }
}
