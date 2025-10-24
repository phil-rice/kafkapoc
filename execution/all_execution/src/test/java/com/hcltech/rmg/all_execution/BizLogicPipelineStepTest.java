package com.hcltech.rmg.all_execution;

import com.hcltech.rmg.appcontainer.interfaces.AppContainer;
import com.hcltech.rmg.common.function.Callback;
import com.hcltech.rmg.config.aspect.AspectMap;
import com.hcltech.rmg.config.bizlogic.BizLogicAspect;
import com.hcltech.rmg.config.config.BehaviorConfig;
import com.hcltech.rmg.config.config.Config;
import com.hcltech.rmg.execution.bizlogic.BizLogicExecutor;
import com.hcltech.rmg.messages.Envelope;
import com.hcltech.rmg.messages.EnvelopeHeader;
import com.hcltech.rmg.messages.ValueEnvelope;
import com.hcltech.rmg.parameters.Parameters;
import com.hcltech.rmg.xml.XmlTypeClass;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/**
 * Tests for the lean BizLogicPipelineStep (async-shaped).
 * These use Mockito to avoid constructing full config/envelope graphs.
 */
class BizLogicPipelineStepTest {

    // ----- Shared fakes / constants -----
    private static final String MODULE = "myModule";
    private static final String PARAM_KEY = "paramA";
    private static final String EVENT = "UserCreated";
    private static final String SCHEMA_KEY = "schema.xsd";

    /** Builds a minimal AppContainer mock with just what the step reads. */
    @SuppressWarnings("unchecked")
    private <ESC, CS, MSG, SCH, FRT, FFR, MP>
    AppContainer<ESC, CS, MSG, SCH, FRT, FFR, MP> containerWith(
            Map<String, Config> keyToCfg,
            BizLogicExecutor<CS, MSG> bizLogic
    ) {
        var container = mock(AppContainer.class);

        when(container.keyToConfigMap()).thenReturn(keyToCfg);
        when(container.bizLogic()).thenReturn(bizLogic);
        when(container.enrichmentExecutor()).thenReturn(null); // not used by the step
        when(container.xml()).thenReturn(mock(XmlTypeClass.class)); // not used
        // schema presence check
        var nameToSchema = Map.of(SCHEMA_KEY, (SCH) new Object());
        when(container.nameToSchemaMap()).thenReturn(nameToSchema);

        // Minimal RootConfig mock to provide xmlSchemaPath()
        var rootCfg = mock(com.hcltech.rmg.config.config.RootConfig.class);
        when(rootCfg.xmlSchemaPath()).thenReturn(SCHEMA_KEY);
        when(container.rootConfig()).thenReturn(rootCfg);

        return container;
    }

    /** Builds a ValueEnvelope mock with provided (paramKey, event) and pass-through methods. */
    @SuppressWarnings("unchecked")
    private <CS, MSG> ValueEnvelope<CS, MSG> veWith(String paramKey, String event) {
        var ve = mock(ValueEnvelope.class);
        var hdr = mock(EnvelopeHeader.class);
        var params = mock(Parameters.class);

        when(params.key()).thenReturn(paramKey);
        when(hdr.parameters()).thenReturn(params);
        when(hdr.eventType()).thenReturn(event);
        when(ve.header()).thenReturn(hdr);

        // keyForModule is used to compose the biz-logic key
        when(ve.keyForModule(anyString())).thenAnswer(inv -> paramKey + ":" + event + ":" + inv.getArgument(0));

        return ve;
    }

    /** Simple callback that captures result or error and lets the test assert. */
    private static final class CapturingCb<T> implements Callback<T> {
        T value; Throwable error;
        @Override public void success(T v) { value = v; }
        @Override public void failure(Throwable t) { error = t; }
    }

    // ----- Tests -----

    @Test
    @DisplayName("Pass-through when envelope is not a ValueEnvelope")
    void passthroughWhenNotValueEnvelope() {
        // Given a non-ValueEnvelope
        @SuppressWarnings("unchecked")
        Envelope<Object, Object> plain = mock(Envelope.class);

        // And a container (bizLogic will not be called)
        @SuppressWarnings("unchecked")
        var biz = (BizLogicExecutor<Object, Object>) mock(BizLogicExecutor.class);
        var container = containerWith(Map.of(), biz);

        var step = new BizLogicPipelineStep<>(container, (Supplier<?>) () -> null, MODULE);

        var cb = new CapturingCb<Envelope<Object, Object>>();

        // When
        step.call(plain, cb);

        // Then
        assertSame(plain, cb.value);
        assertNull(cb.error);
        verifyNoInteractions(biz);
    }

    @Test
    @DisplayName("Pass-through when no Config for parameter key")
    void passthroughWhenNoConfig() {
        // Given a ValueEnvelope with PARAM_KEY/EVENT
        var ve = veWith(PARAM_KEY, EVENT);

        // Container has empty key->Config map
        @SuppressWarnings("unchecked")
        var biz = (BizLogicExecutor<Object, Object>) mock(BizLogicExecutor.class);
        var container = containerWith(Map.of(), biz);
        var step = new BizLogicPipelineStep<>(container, (Supplier<?>) () -> null, MODULE);

        var cb = new CapturingCb<Envelope<Object, Object>>();

        // When
        step.call((Envelope<Object, Object>) ve, cb);

        // Then
        assertSame(ve, cb.value);
        verifyNoInteractions(biz);
    }

    @Test
    @DisplayName("Pass-through when no AspectMap for event")
    void passthroughWhenNoAspectMapForEvent() {
        var ve = veWith(PARAM_KEY, EVENT);

        // Config present but behavior.events doesn't contain EVENT
        var behavior = mock(BehaviorConfig.class);
        when(behavior.events()).thenReturn(Map.of()); // empty

        var cfg = new Config(behavior, null, null);
        @SuppressWarnings("unchecked")
        var biz = (BizLogicExecutor<Object, Object>) mock(BizLogicExecutor.class);
        var container = containerWith(Map.of(PARAM_KEY, cfg), biz);

        var step = new BizLogicPipelineStep<>(container, (Supplier<?>) () -> null, MODULE);
        var cb = new CapturingCb<Envelope<Object, Object>>();

        step.call((Envelope<Object, Object>) ve, cb);

        assertSame(ve, cb.value);
        verifyNoInteractions(biz);
    }

    @Test
    @DisplayName("Pass-through when no BizLogic aspect for module")
    void passthroughWhenNoBizLogicForModule() {
        var ve = veWith(PARAM_KEY, EVENT);

        // AspectMap exists but bizlogic map has no entry for MODULE
        var aspectMap = mock(AspectMap.class);
        when(aspectMap.bizlogic()).thenReturn(Map.of()); // missing module

        var behavior = mock(BehaviorConfig.class);
        when(behavior.events()).thenReturn(Map.of(EVENT, aspectMap));

        var cfg = new Config(behavior, null, null);
        @SuppressWarnings("unchecked")
        var biz = (BizLogicExecutor<Object, Object>) mock(BizLogicExecutor.class);
        var container = containerWith(Map.of(PARAM_KEY, cfg), biz);

        var step = new BizLogicPipelineStep<>(container, (Supplier<?>) () -> null, MODULE);
        var cb = new CapturingCb<Envelope<Object, Object>>();

        step.call((Envelope<Object, Object>) ve, cb);

        assertSame(ve, cb.value);
        verifyNoInteractions(biz);
    }

    @Test
    @DisplayName("Delegates to BizLogic and propagates success")
    void delegatesAndPropagatesSuccess() {
        var veIn = veWith(PARAM_KEY, EVENT);

        // AspectMap contains a BizLogicAspect for MODULE
        var aspect = mock(BizLogicAspect.class);
        var aspectMap = mock(AspectMap.class);
        when(aspectMap.bizlogic()).thenReturn(Map.of(MODULE, aspect));

        var behavior = mock(BehaviorConfig.class);
        when(behavior.events()).thenReturn(Map.of(EVENT, aspectMap));

        var cfg = new Config(behavior, null, null);

        // BizLogicExecutor will call cb.success with modified envelope
        @SuppressWarnings("unchecked")
        var biz = (BizLogicExecutor<Object, Object>) mock(BizLogicExecutor.class);

        // Capture callback passed to bizLogic
        ArgumentCaptor<Callback<Envelope<Object, Object>>> cbCaptor = ArgumentCaptor.forClass(Callback.class);
        doAnswer(inv -> {
            // args: key, aspect, ve, cb
            @SuppressWarnings("unchecked")
            var cb = (Callback<Envelope<Object, Object>>) inv.getArgument(3);
            // simulate transformed envelope
            @SuppressWarnings("unchecked")
            var veOut = (ValueEnvelope<Object, Object>) mock(ValueEnvelope.class);
            cb.success(veOut);
            return null;
        }).when(biz).call(anyString(), same((BizLogicAspect) aspect), same((ValueEnvelope<Object, Object>) veIn), cbCaptor.capture());

        var container = containerWith(Map.of(PARAM_KEY, cfg), biz);
        var step = new BizLogicPipelineStep<>(container, (Supplier<?>) () -> null, MODULE);

        var topCb = new CapturingCb<Envelope<Object, Object>>();
        step.call((Envelope<Object, Object>) veIn, topCb);

        assertNotNull(topCb.value);
        assertNull(topCb.error);

        // Verify correct key was composed
        var expectedKey = veIn.keyForModule(MODULE);
        verify(biz).call(eq(expectedKey), same((BizLogicAspect) aspect), same((ValueEnvelope<Object, Object>) veIn), any());

        // And that bizLogic was invoked exactly once
        verify(biz, times(1)).call(anyString(), any(), any(), any());
    }

    @Test
    @DisplayName("Delegates to BizLogic and propagates failure")
    void delegatesAndPropagatesFailure() {
        var veIn = veWith(PARAM_KEY, EVENT);

        var aspect = mock(BizLogicAspect.class);
        var aspectMap = mock(AspectMap.class);
        when(aspectMap.bizlogic()).thenReturn(Map.of(MODULE, aspect));

        var behavior = mock(BehaviorConfig.class);
        when(behavior.events()).thenReturn(Map.of(EVENT, aspectMap));

        var cfg = new Config(behavior, null, null);

        @SuppressWarnings("unchecked")
        var biz = (BizLogicExecutor<Object, Object>) mock(BizLogicExecutor.class);

        doAnswer(inv -> {
            @SuppressWarnings("unchecked")
            var cb = (Callback<Envelope<Object, Object>>) inv.getArgument(3);
            cb.failure(new RuntimeException("boom"));
            return null;
        }).when(biz).call(anyString(), any(), any(), any());

        var container = containerWith(Map.of(PARAM_KEY, cfg), biz);
        var step = new BizLogicPipelineStep<>(container, (Supplier<?>) () -> null, MODULE);

        var topCb = new CapturingCb<Envelope<Object, Object>>();
        step.call((Envelope<Object, Object>) veIn, topCb);

        assertNull(topCb.value);
        assertNotNull(topCb.error);
        assertTrue(topCb.error instanceof RuntimeException);
        assertTrue(topCb.error.getMessage().toLowerCase().contains("boom"));
    }
}
