package com.hcltech.rmg.performance;

import com.hcltech.rmg.all_execution.AllBizLogic;
import com.hcltech.rmg.appcontainer.impl.AppContainerFactoryForMapStringObject;
import com.hcltech.rmg.appcontainer.interfaces.AppContainer;
import com.hcltech.rmg.appcontainer.interfaces.IAppContainerFactory;
import com.hcltech.rmg.config.configs.Configs;
import com.hcltech.rmg.config.loader.ConfigsBuilder;
import com.hcltech.rmg.config.loader.RootConfigLoader;
import com.hcltech.rmg.flinkadapters.InitialEnvelopeMapFunction;
import com.hcltech.rmg.flinkadapters.KeySniffAndClassify;
import com.hcltech.rmg.kafka.KafkaSourceForFlink;
import com.hcltech.rmg.kafka.WatermarkStrategyProvider;
import com.hcltech.rmg.kafkaconfig.KafkaConfig;
import com.hcltech.rmg.messages.Envelope;
import com.hcltech.rmg.messages.ErrorEnvelope;
import com.hcltech.rmg.messages.RetryEnvelope;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.async.AsyncFunction;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.apache.flink.util.OutputTag;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public final class PerfHarnessMain {

    // we’ll create typed tags *inside* buildPipeline to avoid casts per CEPState type

    public static <CepState, Msg, Schema> Pipeline<CepState, Msg> buildPipeline(Class<IAppContainerFactory<KafkaConfig, CepState, Msg, Schema>> factoryClass, String containerId, int lanes, AsyncFunction<Envelope<CepState, Msg>, Envelope<CepState, Msg>> mainAsync, long asyncTimeoutMillis, Integer asyncParallelismOverride // null -> default to source parallelism
    ) {
        // ---- resolve DI and kafka config ----
        AppContainer<KafkaConfig, CepState, Msg, Schema> app = IAppContainerFactory.resolve(factoryClass, containerId).valueOrThrow();
        final KafkaConfig kafka = app.eventSourceConfig();

        final int totalPartitions = kafka.sourceParallelism();

        // ---- env config ----
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(totalPartitions > 0 ? totalPartitions : env.getParallelism());
        env.getConfig().setAutoWatermarkInterval(0);
        env.enableCheckpointing(30_000);

        // ---- source ----
        var raw = KafkaSourceForFlink.rawKafkaStream(containerId, env, kafka.bootstrapServers(), kafka.topic(), kafka.groupId(), totalPartitions, OffsetsInitializer.earliest(), Duration.ofSeconds(60), WatermarkStrategyProvider.none());

        // ---- pre: sniff key -> (domainId, raw) ; side output only here (optional to keep) ----
        // A typed errors tag specifically for this CEPState + payload type
        OutputTag<ErrorEnvelope<CepState, Msg>> sniffErrorsTag = new OutputTag<>("sniff-errors", TypeInformation.of(new TypeHint<ErrorEnvelope<CepState, Msg>>() {
        })) {
        };

        var sniff = raw.process(new KeySniffAndClassify(containerId, (OutputTag) sniffErrorsTag, lanes)).name("key-sniff"); // SingleOutputStreamOperator<Tuple2<String, RawMessage>>

        var sniffErrors = sniff.getSideOutput(sniffErrorsTag); // keep if you want to observe sniff errors

        // ---- map to Envelope (Value/Error) ----
        var envelopes = sniff.keyBy(t -> t.f0) // domainId
                .map(new InitialEnvelopeMapFunction<>(factoryClass, containerId)).name("to-envelope")
                .map(new ExecutionPipeline<KafkaConfig, CepState, Msg, Schema>(factoryClass, containerId, "notification")); // DataStream<Envelope<CEPState, Map<String,Object>>>

        // ---- async stage (Envelope -> Envelope) ----
        // parallelism and capacity calculation
        int asyncParallelism = (asyncParallelismOverride != null) ? asyncParallelismOverride : totalPartitions;
        int partitionsPerSubtask = (int) Math.ceil((double) totalPartitions / Math.max(1, asyncParallelism));
        int capacity = Math.max(1, (int) Math.round(lanes * partitionsPerSubtask * 1.2)); // +20% headroom

        var processed = AsyncDataStream.orderedWait( // or unorderedWait for higher throughput if ordering isn’t required
                envelopes, mainAsync, asyncTimeoutMillis, TimeUnit.MILLISECONDS, capacity).name("main-async").setParallelism(asyncParallelism);

        // ---- single splitter AFTER async ----
        OutputTag<ErrorEnvelope<CepState, Msg>> errorsTag = new OutputTag<>("errors", TypeInformation.of(new TypeHint<ErrorEnvelope<CepState, Msg>>() {
        })) {
        };
        OutputTag<RetryEnvelope<CepState, Msg>> retriesTag = new OutputTag<>("retries", TypeInformation.of(new TypeHint<RetryEnvelope<CepState, Msg>>() {
        })) {
        };

        var values = processed.process(new SplitEnvelopes<CepState, Msg>(errorsTag, retriesTag)).name("splitter"); // DataStream<ValueEnvelope<CEPState, Map<String,Object>>>

        var allErrors = values.getSideOutput(errorsTag);
        var allRetries = values.getSideOutput(retriesTag);

        // If you want sniff errors unified later, you can union here (types must match).
        // For now, we keep Value/Error/Retry from the splitter.

        return new Pipeline<>(env, values, allErrors, allRetries);
    }

    // Example main wiring
    public static void main(String[] args) throws Exception {
        final String containerId = System.getProperty("app.container", "prod");
        var appContainer = AppContainerFactoryForMapStringObject.resolve(containerId).valueOrThrow();
        final int lanes = 300;

        // a trivial pass-through async (replace with your real async)
        AsyncFunction<Envelope<Map<String, Object>, Map<String, Object>>, Envelope<Map<String, Object>, Map<String, Object>>> mainAsync = new RichAsyncFunction<>() {
            @Override
            public void asyncInvoke(Envelope<Map<String, Object>, Map<String, Object>> input, ResultFuture<Envelope<Map<String, Object>, Map<String, Object>>> result) {
                result.complete(java.util.Collections.singletonList(input));
            }
        };

        Pipeline<Map<String, Object>, Map<String, Object>> pipe = buildPipeline((Class) AppContainerFactoryForMapStringObject.class, containerId, lanes, mainAsync, 2_000, null);

// start time-driven printer (every 2s)
        PerfStats.start(2000);

// attach sinks (metrics + totals)
        pipe.values().addSink(new MetricsCountingSink<>("envelopes", MetricsCountingSink.Kind.VALUES)).name("values-metrics");
        pipe.errors().addSink(new MetricsCountingSink<>("envelopes", MetricsCountingSink.Kind.ERRORS)).name("errors-metrics");
        pipe.retries().addSink(new MetricsCountingSink<>("envelopes", MetricsCountingSink.Kind.RETRIES)).name("retries-metrics");


// brokers + topics
        String brokers = appContainer.eventSourceConfig().bootstrapServers();//System.getProperty("kafka.bootstrap", "localhost:9092");
        String processedTopic = "processed";
        String errorsTopic = "errors";
        String retryTopic = "retry";

// route
        EnvelopeRouting.routeToKafkaWithMetrics(
                pipe.values(),   // DataStream<ValueEnvelope<Object, Map<String,Object>>>
                pipe.errors(),   // DataStream<ErrorEnvelope<Object, Map<String,Object>>>
                pipe.retries(),  // DataStream<RetryEnvelope<Object, Map<String,Object>>>
                brokers, processedTopic, errorsTopic, retryTopic
        );

        pipe.env().execute("rmg-perf-harness");
    }

}
