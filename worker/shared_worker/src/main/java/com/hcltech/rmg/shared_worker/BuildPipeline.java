package com.hcltech.rmg.shared_worker;

import com.hcltech.rmg.appcontainer.interfaces.AppContainer;
import com.hcltech.rmg.appcontainer.interfaces.AppContainerDefn;
import com.hcltech.rmg.appcontainer.interfaces.IAppContainerFactory;
import com.hcltech.rmg.flink_metrics.FlinkMetricsParams;
import com.hcltech.rmg.flinkadapters.AiComparisonFunction;
import com.hcltech.rmg.flinkadapters.EnvelopeAsyncProcessingFunction;
import com.hcltech.rmg.flinkadapters.MakeEmptyValueEnvelope;
import com.hcltech.rmg.kafka.SplitEnvelopes;
import com.hcltech.rmg.kafka.ValueErrorRetryStreams;
import com.hcltech.rmg.kafkaconfig.KafkaConfig;
import com.hcltech.rmg.messages.Envelope;
import com.hcltech.rmg.messages.RawMessage;
import com.hcltech.rmg.messages.ValueEnvelope;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.util.Collector;

public class BuildPipeline {
    public static <CepState, Msg, Schema> ValueErrorRetryStreams<CepState, Msg> buildPipeline(
            StreamExecutionEnvironment env,
            AppContainerDefn<KafkaConfig, CepState, Msg, Schema, RuntimeContext, Collector<Envelope<CepState, Msg>>, FlinkMetricsParams> appContainerDefn,

            boolean rememberBizlogicInput) {
        AppContainer<KafkaConfig, CepState, Msg, Schema, RuntimeContext, Collector<Envelope<CepState, Msg>>, FlinkMetricsParams> app = IAppContainerFactory.resolve(appContainerDefn).valueOrThrow();
        KafkaConfig kafka = app.eventSourceConfig();

        DataStream<RawMessage> raw = KafkaFlinkHelper.createRawMessageStreamFromKafka(
                appContainerDefn, env, kafka, app.checkPointIntervalMillis());
        KeyedStream<RawMessage, String> keyedRaw = raw.keyBy(RawMessage::domainId);

// 2) Map to Envelope
        DataStream<Envelope<CepState, Msg>> withCepState =
                keyedRaw.map(new MakeEmptyValueEnvelope<>(appContainerDefn));

// 3) Key again by domainId (so the operator gets a KeyContext)
        KeyedStream<Envelope<CepState, Msg>, String> keyedEnvelopes =
                withCepState.keyBy(Envelope::domainId);

// 4) Use transform(...) with your OneInputStreamOperator
        OneInputStreamOperator<Envelope<CepState, Msg>, Envelope<CepState, Msg>> fn = new EnvelopeAsyncProcessingFunction<KafkaConfig, CepState, Msg, Schema>(appContainerDefn, "notification", rememberBizlogicInput);

        SingleOutputStreamOperator<Envelope<CepState, Msg>> processedStream =
                keyedEnvelopes.<Envelope<CepState, Msg>>transform(
                                "envelope-async",
                                (TypeInformation<Envelope<CepState, Msg>>) TypeInformation.of(new TypeHint<Envelope<CepState, Msg>>() {
                                }),
                                fn
                        )
                        .uid("envelope-async")        // optional but recommended for savepoints
                        .name("EnvelopeAsyncProcessing");

        var maybeWithAi = rememberBizlogicInput ? processedStream.map(new AiComparisonFunction<>((AppContainerDefn) appContainerDefn)) : processedStream;


        SingleOutputStreamOperator<ValueEnvelope<CepState, Msg>> values = maybeWithAi.process(new SplitEnvelopes<>()).name("splitter");

        return ValueErrorRetryStreams.from(env, values);
    }
}
