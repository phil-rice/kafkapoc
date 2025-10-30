package com.hcltech.rmg.flinkadapters;

import com.hcltech.rmg.appcontainer.interfaces.AppContainerDefn;
import com.hcltech.rmg.appcontainer.interfaces.IAppContainerFactory;
import com.hcltech.rmg.celcore.CelVarType;
import com.hcltech.rmg.celcore.CompiledCelRuleWithDetails;
import com.hcltech.rmg.messages.AiFailureEnvelope;
import com.hcltech.rmg.messages.AiFailureEnvelopeFactory;
import com.hcltech.rmg.messages.Envelope;
import com.hcltech.rmg.messages.ValueEnvelope;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.functions.RichMapFunction;

import java.util.Map;

/**
 * RichMapFunction that compares actual vs expected outputs using a CEL expression.
 * Operates on Envelope<Map<String, Object>, Map<String, Object>>.
 */
public class AiComparisonFunction<EventSourceConfig, Schema, FlinkRT, FlinkFR, MetricsParam>
        extends RichMapFunction<Envelope<Map<String, Object>, Map<String, Object>>,
        Envelope<Map<String, Object>, Map<String, Object>>> {

    private final AppContainerDefn<EventSourceConfig,
            Map<String, Object>,
            Map<String, Object>,
            Schema,
            FlinkRT,
            FlinkFR,
            MetricsParam> containerDefn;
    private transient CompiledCelRuleWithDetails<Object,Object> compiledCel;
    private AiFailureEnvelopeFactory<Map<String, Object>, Map<String, Object>> failureFactory;

    public AiComparisonFunction(
            AppContainerDefn<EventSourceConfig,
                    Map<String, Object>,
                    Map<String, Object>,
                    Schema,
                    FlinkRT,
                    FlinkFR,
                    MetricsParam> containerDefn) {
        this.containerDefn = containerDefn;
    }

    @Override
    public void open(OpenContext parameters) throws Exception {
        var container = IAppContainerFactory.resolve(containerDefn).valueOrThrow();

        this.failureFactory = container.aiFailureEnvelopeFactory();
        String cel = container.celConditionForAi();
        this.compiledCel = container.celBuilder()
                .<Object,Object>createCelRuleBuilder(cel)
                .withVar("output", CelVarType.DYN, m -> m)
                .compile()
                .valueOrThrow();
    }

    @Override
    public Envelope<Map<String, Object>, Map<String, Object>> map(
            Envelope<Map<String, Object>, Map<String, Object>> value) throws Exception {
        if (value instanceof ValueEnvelope<Map<String, Object>, Map<String, Object>> ve) {
            Object actualProjection = compiledCel.executor().execute(ve.data());
            Map<String, Object> cargo = ve.header().cargo();
            Object rawExpected = cargo.get(AiFailureEnvelopeFactory.BIZLOGIC_EXPECTED);
            Object expectedProjection = compiledCel.executor().execute(rawExpected);
            if (!actualProjection.equals(expectedProjection)) {
                AiFailureEnvelope<Map<String, Object>, Map<String, Object>> result = failureFactory.createAiFailureEnvelope(ve, actualProjection, expectedProjection);
                return result;
            }
        }
        return value;
    }

}
