package com.hcltech.rmg.interfaces.envelope;

import com.hcltech.rmg.common.ITimeService;

public interface Envelope<CEPState, T> {
    ValueEnvelopeNew<CEPState, T> valueEnvelope();

    default EnvelopeHeader<CEPState> header() {
        return valueEnvelope().header();
    }

    static <CEPState> EnvelopeHeader<CEPState> createHeader(String domainId,
                                                            long eventStartTime,
                                                            String rawInput,
                                                            String eventType,
                                                            ITimeService timeService) {
        CEPState cepState = null;
        return new EnvelopeHeader<>(domainId, eventType, eventStartTime, timeService.currentTimeMillis(), -1, rawInput, cepState, null);
    }

}
