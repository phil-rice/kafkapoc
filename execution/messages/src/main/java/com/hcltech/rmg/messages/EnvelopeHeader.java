package com.hcltech.rmg.messages;

import com.hcltech.rmg.config.config.BehaviorConfig;
import com.hcltech.rmg.parameters.ParameterExtractor;
import com.hcltech.rmg.parameters.Parameters;

import java.util.Map;

/**
 * Local-only domain envelope that exists *inside a single operator box*
 * (no network hops). It is intentionally not serializable and should
 * never be emitted as a stream element.
 * <p>
 * - parameters: taken from config per message type (runtime-defined)
 * - config: direct reference to the config for this set of parameters
 * - cepState: direct reference to the CEP state for this domainId
 */
public record EnvelopeHeader<CepState>(
        String domainType,
        String eventType, //wil lbe null at start until we have parsed the message and found it
        RawMessage rawMessage,
        Parameters parameters,
//This will be null at start until we have parsed the message and found them. The actual parameters are defined in the config. Includes event type and domain type
        BehaviorConfig config, // the specific combination for this set of parameters
        Map<String, Object> cargo
        //Occasionally we want to carry 'stuff'. Like when doing tests: we might carry the expecter
) {
    <Msg> EnvelopeHeader<CepState> withMessage(ParameterExtractor<Msg> parameterExtractor, Msg message, String eventType) {
        return new EnvelopeHeader<>(
                domainType,
                eventType,
                rawMessage,
                parameterExtractor.parameters(message, eventType, domainType, rawMessage().domainId()).valueOrThrow(),
                config,
                Map.of());
    }

    public EnvelopeHeader<CepState> withCargo(Map<String, Object> cargo) {
        return new EnvelopeHeader<>(domainType, eventType, rawMessage, parameters, config, cargo);
    }


}
