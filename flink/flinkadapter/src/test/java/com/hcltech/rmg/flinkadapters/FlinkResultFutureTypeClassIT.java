package com.hcltech.rmg.flinkadapters;

import com.hcltech.rmg.common.async.FailureAdapter;
import com.hcltech.rmg.common.async.FutureRecordTypeClass;
import com.hcltech.rmg.messages.*;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.apache.flink.streaming.api.functions.sink.legacy.SinkFunction;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

public class FlinkResultFutureTypeClassIT {

    // --------- tiny static collecting sink (Flink 2.0 friendly) ----------
    static final class CollectSink<T> implements SinkFunction<T> {
        static final List<Object> VALUES = new ArrayList<>();
        @Override public synchronized void invoke(T value, Context ctx) { VALUES.add(value); }
        static synchronized <X> List<X> drain() {
            @SuppressWarnings("unchecked")
            List<X> copy = new ArrayList<>((List<X>)(List<?>)VALUES);
            VALUES.clear();
            return copy;
        }
        static synchronized void clear() { VALUES.clear(); }
    }

    @AfterEach
    void tearDown() { CollectSink.clear(); }

    // ---------- helpers to build inputs ----------
    private static EnvelopeHeader<Object> header(String domainId) {
        return new EnvelopeHeader<>(
                "test-domain",
                "test-event",
                new RawMessage("raw", domainId, 1L, 2L, 3, 4L, "tp", "ts", "bg"),
                null,
                null,
                Map.of()
        );
    }

    private static ValueEnvelope<Object, String> ve(String domainId, String data) {
        return new ValueEnvelope<>(header(domainId), data, null, new ArrayList<>());
    }

    // ---------- AsyncFunction that delegates to your adapter ----------
    static final class AdapterDrivenAsyncFn
            extends RichAsyncFunction<Envelope<Object, String>, Envelope<Object, String>> {

        private final FutureRecordTypeClass<ResultFuture<Envelope<Object, String>>,
                Envelope<Object, String>, Envelope<Object, String>> frAdapter;

        AdapterDrivenAsyncFn(FailureAdapter<Envelope<Object, String>, Envelope<Object, String>> failure) {
            this.frAdapter = new FlinkResultFutureTypeClass<>(failure);
        }

        @Override
        public void asyncInvoke(Envelope<Object, String> input, ResultFuture<Envelope<Object, String>> rf) {
            String d = input.valueEnvelope().data();

            // We don’t need a hook here, so pass null.
            if (d.startsWith("ok:")) {
                // success: pass-through the same envelope as Out
                frAdapter.completed(rf, /* onComplete */ null, /* In */ input, /* Out */ input);
            } else if (d.startsWith("fail:")) {
                // failure: adapter will map ErrorEnvelope
                frAdapter.failed(rf, /* onFailed */ null, /* In */ input, new IllegalStateException("boom"));
            } else if (d.startsWith("timeout:")) {
                // timeout: adapter will map RetryEnvelope
                frAdapter.timedOut(rf, /* onTimedOut */ null, /* In */ input, TimeUnit.MILLISECONDS.toNanos(7));
            } else {
                frAdapter.completed(rf, null, input, input);
            }
        }
    }

    @Test
    void resultFuture_is_completed_for_success_failure_timeout() throws Exception {
        final String operatorId = "FlinkResultFutureAdapterIT#op";
        FailureAdapter<Envelope<Object, String>, Envelope<Object, String>> failure =
                EnvelopeFailureAdapter.defaultAdapter(operatorId);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStream<Envelope<Object, String>> src = env.fromElements(
                ve("D1", "ok:a"),       // success
                ve("D2", "fail:x"),     // failure -> ErrorEnvelope
                ve("D3", "timeout:y"),  // timeout -> RetryEnvelope
                ve("D4", "ok:b")        // success
        );

        DataStream<Envelope<Object, String>> out = AsyncDataStream.unorderedWait(
                src,
                new AdapterDrivenAsyncFn(failure),
                2, TimeUnit.SECONDS,
                16
        );

        out.addSink(new CollectSink<>()); // collect all outputs

        // run bounded job to completion
        env.execute("flink-resultfuture-adapter-it");

        // assert results
        List<Envelope<Object, String>> results = CollectSink.drain();

        assertEquals(4, results.size(), "should receive all outputs");

        Envelope<Object, String> okA = results.stream()
                .filter(e -> e instanceof ValueEnvelope && e.valueEnvelope().data().equals("ok:a"))
                .findFirst().orElse(null);
        Envelope<Object, String> okB = results.stream()
                .filter(e -> e instanceof ValueEnvelope && e.valueEnvelope().data().equals("ok:b"))
                .findFirst().orElse(null);
        Envelope<Object, String> errX = results.stream()
                .filter(e -> e instanceof ErrorEnvelope)
                .findFirst().orElse(null);
        Envelope<Object, String> retryY = results.stream()
                .filter(e -> e instanceof RetryEnvelope)
                .findFirst().orElse(null);

        // Successes pass through as ValueEnvelope
        assertNotNull(okA, "ok:a expected");
        assertEquals("D1", okA.valueEnvelope().header().rawMessage().domainId());

        assertNotNull(okB, "ok:b expected");
        assertEquals("D4", okB.valueEnvelope().header().rawMessage().domainId());

        // Failure mapped to ErrorEnvelope
        assertNotNull(errX, "fail:x should map to ErrorEnvelope");
        ErrorEnvelope<Object, String> err = (ErrorEnvelope<Object, String>) errX;
        assertEquals(operatorId, err.stageName());
        assertEquals("D2", err.valueEnvelope().header().rawMessage().domainId());
        assertEquals("fail:x", err.valueEnvelope().data());
        assertNotNull(err.errors());
        assertFalse(err.errors().isEmpty());

        // Timeout mapped to RetryEnvelope
        assertNotNull(retryY, "timeout:y should map to RetryEnvelope");
        RetryEnvelope<Object, String> retry = (RetryEnvelope<Object, String>) retryY;
        assertEquals(operatorId, retry.stageName());
        assertEquals("D3", retry.valueEnvelope().header().rawMessage().domainId());
        assertEquals("timeout:y", retry.valueEnvelope().data());
    }
}
