package com.hcltech.rmg.flinkadapters;

import com.hcltech.rmg.appcontainer.interfaces.AppContainer;
import com.hcltech.rmg.appcontainer.interfaces.AppContainerDefn;
import com.hcltech.rmg.appcontainer.interfaces.IAppContainerFactory;
import com.hcltech.rmg.common.ITimeService;
import com.hcltech.rmg.common.async.*;
import com.hcltech.rmg.common.errorsor.ErrorsOr;
import com.hcltech.rmg.messages.Envelope;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

import java.lang.reflect.Field;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

/** Asserts the operator's periodic processing-time timer calls exec.drain(...). */
public class EnvelopeAsyncPeriodicDrainTest {

    /** Minimal operator: no pipeline, just enough to construct the executor. */
    static final class MinimalOp extends AbstractEnvelopeAsyncProcessingFunction<
            Object, Object, String, Object,
            org.apache.flink.api.common.functions.RuntimeContext, Object> {

        MinimalOp(AppContainerDefn<Object, Object, String, Object,
                org.apache.flink.api.common.functions.RuntimeContext,
                Output<StreamRecord<Envelope<Object, String>>>, Object> defn) {
            super(defn);
        }

        @Override
        protected OrderPreservingAsyncExecutor.UserFnPort<
                Envelope<Object, String>, Envelope<Object, String>,
                Output<StreamRecord<Envelope<Object, String>>>> createUserFnPort(
                AppContainer<Object, Object, String, Object,
                        org.apache.flink.api.common.functions.RuntimeContext,
                        Output<StreamRecord<Envelope<Object, String>>>, Object> c) {
            // No-op async: we won’t submit anything in this test anyway.
            return (tc, in, corr, completion) -> {};
        }

        @Override
        protected void protectedSetupInOpen(AppContainer<Object, Object, String, Object,
                org.apache.flink.api.common.functions.RuntimeContext,
                Output<StreamRecord<Envelope<Object, String>>>, Object> appContainer) {
            // nothing
        }

        @Override
        protected void setKey(Envelope<Object, String> in, Envelope<Object, String> out) {
            // nothing
        }
    }

    @Test
    void timer_calls_executor_drain() throws Exception {
        // --- Mocks for container + async config returned by factory resolution ---
        @SuppressWarnings("unchecked")
        AppContainerDefn<Object, Object, String, Object,
                org.apache.flink.api.common.functions.RuntimeContext,
                Output<StreamRecord<Envelope<Object, String>>>, Object> defn = mock(AppContainerDefn.class);

        @SuppressWarnings("unchecked")
        AppContainer<Object, Object, String, Object,
                org.apache.flink.api.common.functions.RuntimeContext,
                Output<StreamRecord<Envelope<Object, String>>>, Object> container = mock(AppContainer.class);

        // Minimal correlator / adapters for config
        Correlator<Envelope<Object, String>> correlator = new Correlator<>() {
            @Override public String correlationId(Envelope<Object, String> in) { return "cid"; }
            @Override public int laneHash(Envelope<Object, String> in) { return 0; }
        };
        FailureAdapter<Envelope<Object, String>, Envelope<Object, String>> failureAdapter = new FailureAdapter<>() {
            @Override public Envelope<Object, String> onFailure(Envelope<Object, String> input, Throwable error) { return input; }
            @Override public Envelope<Object, String> onTimeout(Envelope<Object, String> in, long elapsedNanos) { return in; }
        };
        FutureRecordTypeClass<Output<StreamRecord<Envelope<Object, String>>>, Envelope<Object, String>, Envelope<Object, String>> fr =
                new FutureRecordTypeClass<>() {
                    @Override public void completed(Output<StreamRecord<Envelope<Object, String>>> out,
                                                    java.util.function.BiConsumer<Envelope<Object, String>, Envelope<Object, String>> hook,
                                                    Envelope<Object, String> in, Envelope<Object, String> outVal) {}
                    @Override public void timedOut(Output<StreamRecord<Envelope<Object, String>>> out,
                                                   java.util.function.BiConsumer<Envelope<Object, String>, Envelope<Object, String>> hook,
                                                   Envelope<Object, String> in, long elapsedNanos) {}
                    @Override public void failed(Output<StreamRecord<Envelope<Object, String>>> out,
                                                 java.util.function.BiConsumer<Envelope<Object, String>, Envelope<Object, String>> hook,
                                                 Envelope<Object, String> in, Throwable error) {}
                };

        when(container.asyncCfg()).thenReturn(new OrderPreservingAsyncExecutorConfig<>(
                /*laneCount*/ 2, /*laneDepth*/ 2, /*maxInFlight*/ 1, /*executorThreads*/ 1, /*timeoutMs*/ 0L,
                correlator, failureAdapter, fr, ITimeService.real));

        // Static-mock the factory to return our container
        try (MockedStatic<IAppContainerFactory> mocked = mockStatic(IAppContainerFactory.class)) {
            var stub = ErrorsOr.lift(container);
            mocked.when(() -> IAppContainerFactory.resolve(any())).thenReturn(stub);

            var op = new MinimalOp(defn);

            try (var harness = new OneInputStreamOperatorTestHarness<
                    Envelope<Object, String>, Envelope<Object, String>>(op)) {

                harness.open(); // this schedules the periodic timer and builds 'exec'

                // Spy on the operator's 'exec' so we can verify drain() calls
                Field execField = AbstractEnvelopeAsyncProcessingFunction.class.getDeclaredField("exec");
                execField.setAccessible(true);
                @SuppressWarnings("unchecked")
                OrderPreservingAsyncExecutor<Envelope<Object, String>, Envelope<Object, String>,
                        Output<StreamRecord<Envelope<Object, String>>>> realExec =
                        (OrderPreservingAsyncExecutor<Envelope<Object, String>, Envelope<Object, String>,
                                Output<StreamRecord<Envelope<Object, String>>>>) execField.get(op);

                @SuppressWarnings("unchecked")
                var spyExec = (OrderPreservingAsyncExecutor<Envelope<Object, String>, Envelope<Object, String>,
                        Output<StreamRecord<Envelope<Object, String>>>>) spy(realExec);

                execField.set(op, spyExec); // swap in the spy

                // Advance processing time beyond ~500ms interval to trigger the scheduled drain
                long t = harness.getProcessingTime();
                harness.setProcessingTime(t + 600);

                // Verify drain() was called at least once by the timer
                verify(spyExec, atLeastOnce()).drain(any(), any());

                // Advance once more to show repeated scheduling works
                harness.setProcessingTime(t + 1200);
                verify(spyExec, atLeast(2)).drain(any(), any());
            }
        }
    }
}
