// put this next to PerfHarnessMain or in its own file
package com.hcltech.rmg.kafka;

import com.hcltech.rmg.messages.AiFailureEnvelope;
import com.hcltech.rmg.messages.ErrorEnvelope;
import com.hcltech.rmg.messages.RetryEnvelope;
import com.hcltech.rmg.messages.ValueEnvelope;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public record ValueErrorRetryStreams<CepState, Msg>(
        StreamExecutionEnvironment env,
        DataStream<ValueEnvelope<CepState, Msg>> values,
        DataStream<ErrorEnvelope<CepState, Msg>> errors,
        DataStream<RetryEnvelope<CepState, Msg>> retries,
        DataStream<AiFailureEnvelope<CepState, Msg>> aiFailures
) {

    public static <CepState, Msg> ValueErrorRetryStreams<CepState, Msg> from(StreamExecutionEnvironment env,
                                                                             SingleOutputStreamOperator<ValueEnvelope<CepState, Msg>> values) {

        DataStream<ErrorEnvelope<CepState, Msg>> allErrors = (DataStream) values.getSideOutput(EnvelopeOutputTags.ERRORS);
        DataStream<RetryEnvelope<CepState, Msg>> allRetries = (DataStream) values.getSideOutput(EnvelopeOutputTags.RETRIES);
        DataStream<AiFailureEnvelope<CepState, Msg>> allAIFailures = (DataStream) values.getSideOutput(EnvelopeOutputTags.AI_FAILURES);
        return new ValueErrorRetryStreams<CepState, Msg>(env, values, allErrors, allRetries, allAIFailures);
    }
}
