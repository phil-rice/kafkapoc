// put this next to PerfHarnessMain or in its own file
package com.hcltech.rmg.performance;

import com.hcltech.rmg.messages.ErrorEnvelope;
import com.hcltech.rmg.messages.RetryEnvelope;
import com.hcltech.rmg.messages.ValueEnvelope;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Map;

public record Pipeline<CepState, Msg>(
        StreamExecutionEnvironment env,
        DataStream<ValueEnvelope<CepState, Msg>> values,
        DataStream<ErrorEnvelope<CepState, Msg>> errors,
        DataStream<RetryEnvelope<CepState, Msg>> retries
) {
}
