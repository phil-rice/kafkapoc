package com.hcltech.rmg.messages;

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
    /**
     * At the beginning we haven't parsed the message yet, so we only have the raw message and domainId.
     * We don't have parameters or config or eventType yet (derived from parsed message), so those are null.
     * The cepState has been found because we have the domainId, so we can get it from that.
     */
    EnvelopeHeader<CepState> createEnvelopeHeaderAtStart(
            RawMessage rawMessage,
            String domainId
    );

}


