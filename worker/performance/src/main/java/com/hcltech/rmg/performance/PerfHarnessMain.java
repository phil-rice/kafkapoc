package com.hcltech.rmg.performance;

import com.hcltech.rmg.appcontainer.impl.AppContainerFactoryForMapStringObject;
import com.hcltech.rmg.appcontainer.interfaces.AppContainer;
import com.hcltech.rmg.appcontainer.interfaces.AppContainerDefn;
import com.hcltech.rmg.appcontainer.interfaces.IAppContainerFactory;
import com.hcltech.rmg.flink_metrics.FlinkMetricsParams;
import com.hcltech.rmg.flinkadapters.*;
import com.hcltech.rmg.kafka.KafkaHelpers;
import com.hcltech.rmg.kafka.KafkaTopics;
import com.hcltech.rmg.kafka.SplitEnvelopes;
import com.hcltech.rmg.kafka.ValueErrorRetryStreams;
import com.hcltech.rmg.kafkaconfig.KafkaConfig;
import com.hcltech.rmg.messages.Envelope;
import com.hcltech.rmg.messages.RawMessage;
import com.hcltech.rmg.messages.ValueEnvelope;
import com.hcltech.rmg.shared_worker.EnvelopeRouting;
import com.hcltech.rmg.shared_worker.KafkaFlinkHelper;
import dev.cel.checker.Env;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperatorFactory;
import org.apache.flink.util.Collector;

import java.util.Map;

public final class PerfHarnessMain {

    /**
     * Build the pipeline using the provided environment (no internal getExecutionEnvironment).
     */
    public static <CepState, Msg, Schema> ValueErrorRetryStreams<CepState, Msg> buildPipeline(
            StreamExecutionEnvironment env,
            AppContainerDefn<KafkaConfig, CepState, Msg, Schema, RuntimeContext, Collector<Envelope<CepState, Msg>>, FlinkMetricsParams> appContainerDefn,
            RichAsyncFunction<Envelope<CepState, Msg>, Envelope<CepState, Msg>> func,
            int lanes, long asyncTimeoutMillis) {
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
        OneInputStreamOperator<Envelope<CepState, Msg>, Envelope<CepState, Msg>> fn = new EnvelopeAsyncProcessingFunction<KafkaConfig, CepState, Msg, Schema>(appContainerDefn, "notification");

        SingleOutputStreamOperator<Envelope<CepState, Msg>> processedStream =
                keyedEnvelopes.<Envelope<CepState, Msg>>transform(
                                "envelope-async",
                                (TypeInformation<Envelope<CepState, Msg>>) TypeInformation.of(new TypeHint<Envelope<CepState, Msg>>() {
                                }),
                                fn
                        )
                        .uid("envelope-async")        // optional but recommended for savepoints
                        .name("EnvelopeAsyncProcessing");


        SingleOutputStreamOperator<ValueEnvelope<CepState, Msg>> values = processedStream.process(new SplitEnvelopes<>()).name("splitter");

        return ValueErrorRetryStreams.from(env, values);
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = FlinkHelper.makeDefaultFlinkConfig();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);
        var appContainerDefn = AppContainerDefn.of(AppContainerFactoryForMapStringObject.class, "prod");
        var appContainer = IAppContainerFactory.resolve(appContainerDefn).valueOrThrow();
        if (KafkaTopics.ensureTopics(appContainer.eventSourceConfig(), EnvelopeRouting.allTopics, 12, (short) 1).valueOrThrow()) {//just sticking 12/3 in for tests
            System.out.println("Created output topics");
        }
        final int lanes = 300;
        int asyncTimeoutMillis = 2_000;
        var func = new NormalPipelineFunction<>(appContainerDefn, "notification");
        ValueErrorRetryStreams<Map<String, Object>, Map<String, Object>> pipe = buildPipeline(env, appContainerDefn, func, lanes, asyncTimeoutMillis);

        // optional: try to locate the actual /metrics port in background
        new Thread(() -> FlinkHelper.probeMetricsPort(9400, 9401), "metrics-probe").start();

        // perf printer
        PerfStats.start(2000);

        // sinks
        pipe.values().addSink(new MetricsCountingSink<>("envelopes", MetricsCountingSink.Kind.VALUES)).name("values-metrics");
        pipe.errors().addSink(new MetricsCountingSink<>("envelopes", MetricsCountingSink.Kind.ERRORS)).name("errors-metrics");
        pipe.retries().addSink(new MetricsCountingSink<>("envelopes", MetricsCountingSink.Kind.RETRIES)).name("retries-metrics");

        // route to Kafka
        String brokers = appContainer.eventSourceConfig().bootstrapServer();
        EnvelopeRouting.routeToKafka(pipe.values(), pipe.errors(), pipe.retries(), brokers, "processed", "errors", "retry");

        pipe.env().execute("rmg-perf-harness");
    }


}
