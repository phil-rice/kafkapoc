package com.hcltech.rmg.messages;

import com.hcltech.rmg.config.config.BehaviorConfig;
import com.hcltech.rmg.parameters.Parameters;

import java.util.Map;
import java.util.Objects;

/**
 * Local-only domain envelope that exists *inside a single operator box*
 * (no network hops). It is intentionally not serializable and should
 * never be emitted as a stream element.
 * <p>
 * - parameters: taken from config per message type (runtime-defined)
 * - config: direct reference to the config for this set of parameters
 * - cepState: direct reference to the CEP state for this domainId
 */
public interface EnvelopeHeaderFactory<CepState> {
    /** At the beginning we haven't parsed the message yet, so we only have the raw message and domainId.
     * We don't have parameters or config or eventType yet (derived from parsed message), so those are null.
     * The cepState has been found because we have the domainId, so we can get it from that.
     */
    EnvelopeHeader<CepState> createEnvelopeHeaderAtStart(
           RawMessage rawMessage,
            String domainId,
            CepState cepState
    );
}

class InitialEnvelopeHeaderFactory<CepState> implements EnvelopeHeaderFactory<CepState> {
    private final String domainType;

    public InitialEnvelopeHeaderFactory(String domainType) {
        this.domainType = domainType;
        Objects.requireNonNull(domainType, "domainType cannot be null");
    }

    @Override
    public EnvelopeHeader<CepState> createEnvelopeHeaderAtStart(RawMessage rawMessage, String domainId, CepState cepState) {
        Objects.requireNonNull(rawMessage, "rawMessage cannot be null");
        Objects.requireNonNull(domainId, "domainId cannot be null");
        Objects.requireNonNull(cepState, "cepState cannot be null");
        return new EnvelopeHeader<>(domainType,domainId, null,rawMessage, null, null, cepState);
    }
}