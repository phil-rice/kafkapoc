package com.hcltech.rmg.flinkadapters;

import com.hcltech.rmg.all_execution.BizLogicPipelineStep;
import com.hcltech.rmg.all_execution.EnrichmentPipelineStep;
import com.hcltech.rmg.all_execution.ParseMessagePipelineStep;
import com.hcltech.rmg.common.ITimeService;
import com.hcltech.rmg.flink_metrics.FlinkMetricsParams;
import com.hcltech.rmg.messages.Envelope;
import com.hcltech.rmg.messages.EnvelopeHeader;
import com.hcltech.rmg.messages.ValueEnvelope;
import com.hcltech.rmg.metrics.EnvelopeMetrics;
import com.hcltech.rmg.metrics.Metrics;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@SuppressWarnings({"unchecked","rawtypes"})
class NormalPipelineFunctionTest {

    // Generic params for the operator under test
    static class MSC {}
    static class RT {}
    static class FR {}
    static class Schema {}

    private NormalPipelineFunction<MSC, String, java.util.Map<String,Object>, RT, FR, Schema> fn;

    // Injected/mocked collaborators
    private ParseMessagePipelineStep<MSC, String, java.util.Map<String,Object>, Schema, RT, FR, FlinkMetricsParams> parser;
    private EnrichmentPipelineStep<MSC, String, java.util.Map<String,Object>,   Schema, RT, FR, FlinkMetricsParams> enrichment;
    private BizLogicPipelineStep<MSC, String, java.util.Map<String,Object>,     Schema, RT, FR, FlinkMetricsParams> bizLogic;
    private EnvelopeMetrics<Envelope<?,?>> envelopeMetrics;
    private Metrics metrics;
    private ITimeService time;

    @BeforeEach
    void setUp() {
        fn = new NormalPipelineFunction<>(null, "mod");

        parser          = mock(ParseMessagePipelineStep.class);
        enrichment      = mock(EnrichmentPipelineStep.class);
        bizLogic        = mock(BizLogicPipelineStep.class);
        envelopeMetrics = mock(EnvelopeMetrics.class);
        metrics         = mock(Metrics.class);
        time            = mock(ITimeService.class);

        // Inject mocks without reflection
        fn.initForTest(parser, enrichment, bizLogic, envelopeMetrics, metrics, time);
    }

    @Test
    void happyPath_enrich_then_biz_then_complete_setsMetricsAndDuration() throws Exception {
        // Arrange input and staged outputs
        EnvelopeHeader<String> hdr = new EnvelopeHeader<>("dom","evt", null, null, null, java.util.Map.of());
        ValueEnvelope<String, java.util.Map<String,Object>> inEnv  =
                new ValueEnvelope<>(hdr, java.util.Map.of("k","v0"), "S0", java.util.List.of());
        ValueEnvelope<String, java.util.Map<String,Object>> enrEnv =
                new ValueEnvelope<>(hdr, java.util.Map.of("k","v1"), "S1", java.util.List.of());
        ValueEnvelope<String, java.util.Map<String,Object>> bizEnv =
                spy(new ValueEnvelope<>(hdr, java.util.Map.of("k","v2"), "S2", java.util.List.of()));

        when(time.currentTimeNanos()).thenReturn(1_000_000L, 1_250_000L); // dur = 250_000 ns
        when(parser.parse(any())).thenReturn(inEnv);

        // enrichment: succeed with enrEnv
        doAnswer(inv -> { var cb = (com.hcltech.rmg.common.function.Callback) inv.getArgument(1);
            cb.success(enrEnv); return null; })
                .when(enrichment).call(eq(inEnv), any());

        // biz logic: succeed with bizEnv
        doAnswer(inv -> { var cb = (com.hcltech.rmg.common.function.Callback) inv.getArgument(1);
            cb.success(bizEnv); return null; })
                .when(bizLogic).call(eq(enrEnv), any());

        CapturingResultFuture<Envelope<String,java.util.Map<String,Object>>> rf = new CapturingResultFuture<>();

        // Act
        fn.asyncInvoke(inEnv, (ResultFuture) rf);

        // Assert output
        var results = rf.awaitSuccess();
        assertEquals(1, results.size());
        assertSame(bizEnv, results.get(0));

        // Metrics called
        verify(envelopeMetrics).addToMetricsAtEnd(bizEnv);
        // BUGFIX: metric name now ends with ".nanos" and value is in nanos
        verify(metrics).histogram(eq("NormalPipelineFunction.asyncInvoke.nanos"), eq(250_000L));

        // Duration set
        verify(bizEnv).setDurationNanos(250_000L);

        verify(parser).parse(inEnv);
        verify(enrichment).call(eq(inEnv), any());
        verify(bizLogic).call(eq(enrEnv), any());
    }

    @Test
    void enrichmentFailure_shortCircuitsToError_noBizNoMetrics() throws Exception {
        EnvelopeHeader<String> hdr = new EnvelopeHeader<>("dom","evt", null, null, null, java.util.Map.of());
        ValueEnvelope<String, java.util.Map<String,Object>> inEnv =
                new ValueEnvelope<>(hdr, java.util.Map.of("k","v0"), "S0", java.util.List.of());

        when(time.currentTimeNanos()).thenReturn(10L, 20L);
        when(parser.parse(any())).thenReturn(inEnv);

        RuntimeException boom = new RuntimeException("boom");
        doAnswer(inv -> { var cb = (com.hcltech.rmg.common.function.Callback) inv.getArgument(1);
            cb.failure(boom); return null; })
                .when(enrichment).call(eq(inEnv), any());

        CapturingResultFuture<Envelope<String,java.util.Map<String,Object>>> rf = new CapturingResultFuture<>();

        fn.asyncInvoke(inEnv, (ResultFuture) rf);

        Throwable err = rf.awaitFailure();
        assertSame(boom, err);

        verifyNoInteractions(bizLogic);
        verifyNoInteractions(envelopeMetrics);
        verify(metrics, never()).histogram(anyString(), anyLong());
    }

    @Test
    void nonValueEnvelope_skipsSteps_passesThrough_and_recordsLatency_noDuration() throws Exception {
        Envelope<String, java.util.Map<String,Object>> in = mock(Envelope.class);

        when(time.currentTimeNanos()).thenReturn(100L, 400L); // dur = 300 ns
        when(parser.parse(any())).thenReturn((Envelope) in);

        CapturingResultFuture<Envelope<String,java.util.Map<String,Object>>> rf = new CapturingResultFuture<>();

        fn.asyncInvoke((Envelope) in, (ResultFuture) rf);

        var results = rf.awaitSuccess();
        assertEquals(1, results.size());
        assertSame(in, results.get(0));

        verifyNoInteractions(enrichment);
        verifyNoInteractions(bizLogic);

        verify(envelopeMetrics).addToMetricsAtEnd(in);
        verify(metrics).histogram(eq("NormalPipelineFunction.asyncInvoke.nanos"), eq(300L));
        // cannot verify setDurationNanos; not a ValueEnvelope
    }

    // ---- helpers ----

    private static final class CapturingResultFuture<T> implements ResultFuture<T> {
        private final java.util.concurrent.CountDownLatch latch =
                new java.util.concurrent.CountDownLatch(1);
        private java.util.List<T> results;
        private Throwable error;

        @Override public void complete(java.util.Collection<T> result) {
            this.results = List.copyOf(result);
            this.latch.countDown();
        }

        @Override public void completeExceptionally(Throwable error) {
            this.error = error;
            this.latch.countDown();
        }

        java.util.List<T> awaitSuccess() throws InterruptedException {
            assertTrue(latch.await(2, java.util.concurrent.TimeUnit.SECONDS), "timeout");
            if (error != null) fail("Expected success but failed with: " + error);
            return results;
        }

        Throwable awaitFailure() throws InterruptedException {
            assertTrue(latch.await(2, java.util.concurrent.TimeUnit.SECONDS), "timeout");
            assertNotNull(error, "expected failure");
            return error;
        }
    }
}
