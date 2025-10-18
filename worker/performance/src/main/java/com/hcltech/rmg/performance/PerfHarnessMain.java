package com.hcltech.rmg.performance;

import com.hcltech.rmg.appcontainer.impl.AppContainerFactoryForMapStringObject;
import com.hcltech.rmg.appcontainer.interfaces.AppContainer;
import com.hcltech.rmg.appcontainer.interfaces.AppContainerDefn;
import com.hcltech.rmg.appcontainer.interfaces.IAppContainerFactory;
import com.hcltech.rmg.flink_metrics.FlinkMetricsParams;
import com.hcltech.rmg.flinkadapters.FlinkHelper;
import com.hcltech.rmg.flinkadapters.MakeEmptyValueEnvelopeWithCepStateFunction;
import com.hcltech.rmg.flinkadapters.NormalPipelineFunction;
import com.hcltech.rmg.kafka.KafkaHelpers;
import com.hcltech.rmg.kafka.SplitEnvelopes;
import com.hcltech.rmg.kafka.ValueErrorRetryStreams;
import com.hcltech.rmg.kafkaconfig.KafkaConfig;
import com.hcltech.rmg.messages.Envelope;
import com.hcltech.rmg.messages.RawMessage;
import com.hcltech.rmg.messages.ValueEnvelope;
import com.hcltech.rmg.shared_worker.EnvelopeRouting;
import com.hcltech.rmg.shared_worker.KafkaFlinkHelper;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.codehaus.stax2.validation.XMLValidationSchema;

import java.util.Map;

public final class PerfHarnessMain {


    /**
     * Build the pipeline using the provided environment (no internal getExecutionEnvironment).
     */
    public static <CepState, Msg, Schema> ValueErrorRetryStreams<CepState, Msg> buildPipeline(
            StreamExecutionEnvironment env,
            AppContainerDefn<KafkaConfig, CepState, Msg, Schema, FlinkMetricsParams> appContainerDefn,
            RichAsyncFunction<Envelope<CepState, Msg>, Envelope<CepState, Msg>> func,
            int lanes, long asyncTimeoutMillis) {
        AppContainer<KafkaConfig, CepState, Msg, Schema, FlinkMetricsParams> app = IAppContainerFactory.resolve(appContainerDefn).valueOrThrow();
        KafkaConfig kafka = app.eventSourceConfig();

        var raw = KafkaFlinkHelper.createRawMessageStreamFromKafka(appContainerDefn, env, kafka, app.checkPointIntervalMillis());
        DataStream<RawMessage> keyedStream = raw.keyBy(RawMessage::domainId);
        var withCepState = keyedStream.map(new MakeEmptyValueEnvelopeWithCepStateFunction<>(appContainerDefn));
        var outputStream = KafkaHelpers.liftFunctionToOrderedAsync(withCepState, "main-async", func, kafka.sourceParallelism(), lanes, asyncTimeoutMillis);
        SingleOutputStreamOperator<ValueEnvelope<CepState, Msg>> values = outputStream.process(new SplitEnvelopes<>()).name("splitter");
        return ValueErrorRetryStreams.from(env, values);
    }


    public static void main(String[] args) throws Exception {
        Configuration conf = FlinkHelper.makeDefaultFlinkConfig();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);
        var appContainerDefn = AppContainerDefn.of(AppContainerFactoryForMapStringObject.class, "prod");
        var appContainer = IAppContainerFactory.resolve(appContainerDefn).valueOrThrow();
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
        String brokers = appContainer.eventSourceConfig().bootstrapServers();
        EnvelopeRouting.routeToKafka(pipe.values(), pipe.errors(), pipe.retries(), brokers, "processed", "errors", "retry");

        pipe.env().execute("rmg-perf-harness");
    }


}
