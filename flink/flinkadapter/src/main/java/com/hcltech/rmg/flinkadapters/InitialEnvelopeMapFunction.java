package com.hcltech.rmg.flinkadapters;

import com.hcltech.rmg.common.errorsor.ErrorsOr;
import com.hcltech.rmg.messages.Envelope;
import com.hcltech.rmg.messages.InitialEnvelopeFactory;
import com.hcltech.rmg.messages.RawMessage;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;

/**
 * In:  (domainId, raw)
 * Out: Envelope<CepState, Map<String,Object>>
 * <p>
 * All error paths are wrapped as ErrorEnvelope by the factory (via recover()).
 */
public final class InitialEnvelopeMapFunction<Schema, CepState>
        extends RichMapFunction<Tuple2<String, RawMessage>, Envelope<CepState, java.util.Map<String, Object>>> {

    private final InitialEnvelopeFactory<Schema, CepState> factory;

    public InitialEnvelopeMapFunction(InitialEnvelopeFactory<Schema, CepState> factory) {
        this.factory = factory;
    }

    @Override
    public void open(OpenContext parameters) {
    }

    @Override
    public Envelope<CepState, java.util.Map<String, Object>> map(Tuple2<String, RawMessage> in) {
        String domainId = in.f0;
        RawMessage raw = in.f1;

        // Factory already recover()s to an ErrorEnvelope, so valueOrThrow() is safe
        ErrorsOr<Envelope<CepState, java.util.Map<String, Object>>> eo =
                factory.createEnvelopeHeaderAtStart(raw, domainId);

        return eo.valueOrThrow();
    }
}
