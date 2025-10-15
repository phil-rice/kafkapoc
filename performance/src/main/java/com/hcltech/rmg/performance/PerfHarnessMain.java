package com.hcltech.rmg.performance;

import com.hcltech.rmg.appcontainer.impl.AppContainerFactoryForMapStringObject;
import com.hcltech.rmg.appcontainer.interfaces.AppContainer;
import com.hcltech.rmg.appcontainer.interfaces.IAppContainerFactory;
import com.hcltech.rmg.flinkadapters.MakeEmptyValueEnvelopeFunction;
import com.hcltech.rmg.flinkadapters.NormalPipelineFunction;
import com.hcltech.rmg.kafka.KafkaSourceForFlink;
import com.hcltech.rmg.kafka.WatermarkStrategyProvider;
import com.hcltech.rmg.kafkaconfig.KafkaConfig;
import com.hcltech.rmg.messages.Envelope;
import com.hcltech.rmg.messages.ErrorEnvelope;
import com.hcltech.rmg.messages.RawMessage;
import com.hcltech.rmg.messages.RetryEnvelope;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.OutputTag;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public final class PerfHarnessMain {

    // we’ll create typed tags *inside* buildPipeline to avoid casts per CEPState type

    public static <CepState, Msg, Schema> Pipeline<CepState, Msg> buildPipeline(Class<IAppContainerFactory<KafkaConfig, CepState, Msg, Schema>> factoryClass, String containerId, String module, int lanes, long asyncTimeoutMillis, Integer asyncParallelismOverride // null -> default to source parallelism
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

        // ---- map to Envelope (Value/Error) ----

        DataStream<RawMessage> keyedStream = raw.keyBy(RawMessage::domainId); // domainId

        // ---- async stage (Envelope -> Envelope) ----
        // parallelism and capacity calculation
        int asyncParallelism = (asyncParallelismOverride != null) ? asyncParallelismOverride : totalPartitions;
        int partitionsPerSubtask = (int) Math.ceil((double) totalPartitions / Math.max(1, asyncParallelism));
        int capacity = Math.max(1, (int) Math.round(lanes * partitionsPerSubtask * 1.2)); // +20% headroom

        var withCepState = keyedStream.map(new MakeEmptyValueEnvelopeFunction<>(factoryClass, containerId));
        var processed = AsyncDataStream.<Envelope<CepState, Msg>, Envelope<CepState, Msg>>orderedWait(
                withCepState, new NormalPipelineFunction<>(factoryClass, containerId, module), asyncTimeoutMillis, TimeUnit.MILLISECONDS, capacity).name("main-async").setParallelism(asyncParallelism);

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


        Pipeline<Map<String, Object>, Map<String, Object>> pipe = buildPipeline((Class) AppContainerFactoryForMapStringObject.class, containerId, "notification", lanes, 2_000, null);

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
