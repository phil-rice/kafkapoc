package com.hcltech.rmg.messages;

import com.hcltech.rmg.common.async.Correlator;

public class EnvelopeCorrelator<CepState,Msg> implements Correlator<Envelope<CepState,Msg>> {
    @Override
    public String correlationId(Envelope<CepState, Msg> envelope) {
        return envelope.header().rawMessage().traceparent();
    }

    @Override
    public int laneHash(Envelope<CepState, Msg> envelope) {
        return envelope.domainId().hashCode();
    }
}
