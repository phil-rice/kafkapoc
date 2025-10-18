package com.hcltech.rmg.messages;

import com.hcltech.rmg.cepstate.CepEvent;
import com.hcltech.rmg.cepstate.CepStateTypeClass;
import com.hcltech.rmg.common.errorsor.Value;
import com.hcltech.rmg.execution.aspects.AspectExecutor;

import java.util.List;
import java.util.Objects;
import java.util.function.Function;

public final class ValueEnvelope<CepState, Msg> implements Envelope<CepState, Msg> {
    private EnvelopeHeader<CepState> header;
    private Msg data;
    private CepState cepState;
    private final List<CepEvent> cepStateModifications;

    public ValueEnvelope(EnvelopeHeader<CepState> header,
                         Msg data,
                         CepState cepState,
                         List<CepEvent> cepStateModifications) {
        this.header = header;
        this.data = data;
        this.cepState = cepState;
        this.cepStateModifications = cepStateModifications;
    }

    public ValueEnvelope<CepState, Msg> withData(Msg newData) {
        return new ValueEnvelope<>(header, newData, cepState, cepStateModifications);
    }

    @Override
    public Envelope<CepState, Msg> map(Function<ValueEnvelope<CepState, Msg>, Envelope<CepState, Msg>> mapper) {
        try {
            return mapper.apply(this);
        } catch (Exception e) {
            return new ErrorEnvelope<CepState, Msg>(this, "ValueEnvelope.map", List.of("Exception in map: " + getClass().getSimpleName() + "/" + e.getMessage()));
        }
    }


    public ValueEnvelope<CepState, Msg> withNewCepEvent(CepStateTypeClass<CepState> cepStateTypeClass, CepEvent event) {
        if (event == null) return this;
        var newCepState = cepStateTypeClass.processState(cepState, event);
        cepStateModifications.add(event);
        return new ValueEnvelope<>(header, data, newCepState, cepStateModifications);

    }

    @Override
    public ValueEnvelope<CepState, Msg> valueEnvelope() {
        return this;
    }

    @Override
    public EnvelopeHeader<CepState> header() {
        return header;
    }

    public Msg data() {
        return data;
    }

    public CepState cepState() {
        return cepState;
    }

    public void setHeader(EnvelopeHeader<CepState> header) {
        this.header = header;
    }

    public void setData(Msg data) {
        this.data = data;
    }

    public void setCepState(CepState cepState) {
        this.cepState = cepState;
    }

    public List<CepEvent> cepStateModifications() {
        return cepStateModifications;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) return true;
        if (obj == null || obj.getClass() != this.getClass()) return false;
        var that = (ValueEnvelope) obj;
        return Objects.equals(this.header, that.header) &&
                Objects.equals(this.data, that.data) &&
                Objects.equals(this.cepState, that.cepState) &&
                Objects.equals(this.cepStateModifications, that.cepStateModifications);
    }

    @Override
    public int hashCode() {
        return Objects.hash(header, data, cepState, cepStateModifications);
    }

    @Override
    public String toString() {
        return "ValueEnvelope[" +
                "header=" + header + ", " +
                "data=" + data + ", " +
                "cepState=" + cepState + ", " +
                "cepStateModifications=" + cepStateModifications + ']';
    }

}
