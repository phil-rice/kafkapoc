package com.hcltech.rmg.messages;

import com.hcltech.rmg.cepstate.CepEvent;
import com.hcltech.rmg.cepstate.CepStateTypeClass;

import java.util.List;

public record ValueEnvelope<CepState, Msg>(EnvelopeHeader<CepState> header,
                                           Msg data,
                                           List<CepEvent> cepStateModifications) implements Envelope<CepState, Msg> {
    public ValueEnvelope<CepState, Msg> withData(Msg newData) {
        return new ValueEnvelope<>(header, newData, cepStateModifications);
    }

    public ValueEnvelope<CepState, Msg> withNewCepEvent(CepStateTypeClass<CepState> cepStateTypeClass, CepEvent event) {
        if (event == null) return this;
        var newCepState = cepStateTypeClass.processState(header.cepState(), event);
        var newHeader = header.withCepState(newCepState);
        cepStateModifications.add(event);
        return new ValueEnvelope<>(newHeader, data, cepStateModifications);

    }

    @Override
    public ValueEnvelope<CepState, Msg> valueEnvelope() {
        return this;
    }
}
