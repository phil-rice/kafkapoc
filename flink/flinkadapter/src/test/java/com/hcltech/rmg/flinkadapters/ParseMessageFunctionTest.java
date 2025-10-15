package com.hcltech.rmg.flinkadapters;

import com.hcltech.rmg.messages.Envelope;
import com.hcltech.rmg.messages.EnvelopeHeader;
import com.hcltech.rmg.messages.InitialEnvelopeFactory;
import com.hcltech.rmg.messages.RawMessage;
import com.hcltech.rmg.messages.ValueEnvelope;
import org.apache.flink.api.java.tuple.Tuple2;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.lang.reflect.Field;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.when;

class ParseMessageFunctionTest {

    @Test
    @DisplayName("map(): throws if open() not called (factory is null)")
    void map_before_open_throws() {
        // Use raw Object types to keep generics frictionless
        ParseMessageFunction<Object, Object, Object, Object> fn =
                new ParseMessageFunction<>(null, "container-id");

        var rec = Tuple2.of("dom-1", new RawMessage("<x/>", 1L, 2L, 3, 4L, null, null, null));

        NullPointerException ex = assertThrows(NullPointerException.class, () -> fn.map(rec));
        assertTrue(ex.getMessage().contains("InitialEnvelopeMapFunction not opened"));
    }

    @Test
    @DisplayName("map(): delegates to InitialEnvelopeFactory.createEnvelopeHeaderAtStart(...) and returns its result")
    void map_delegates_to_factory() throws Exception {
        // Arrange: a mocked InitialEnvelopeFactory and inject it into the private field
        @SuppressWarnings("unchecked")
        InitialEnvelopeFactory<Object, String, Object> mockFactory = Mockito.mock(InitialEnvelopeFactory.class);

        ParseMessageFunction<Object, Object, String, Object> fn =
                new ParseMessageFunction<>(null, "container-id");

        // Reflectively set the private 'factory' field
        Field f = ParseMessageFunction.class.getDeclaredField("factory");
        f.setAccessible(true);
        f.set(fn, mockFactory);

        String domainId = "D-42";
        RawMessage raw = new RawMessage("{\"a\":1}", 111L, 222L, 3, 4L, "tp", null, null);
        var in = Tuple2.of(domainId, raw);

        // Expected envelope returned by the factory
        var header = new EnvelopeHeader<Object>("domType", domainId, null, raw, null, null, null);
        Envelope<Object, String> expected = new ValueEnvelope<>(header, raw.rawValue(), List.of());

        when(mockFactory.createEnvelopeHeaderAtStart(raw, domainId)).thenReturn(expected);

        // Act
        Envelope<Object, String> out = fn.map(in);

        // Assert: same instance returned; domainId/raw are wired through
        assertSame(expected, out, "map(...) should return the factory result");
        assertEquals(domainId, out.valueEnvelope().header().domainId());
        assertSame(raw, out.valueEnvelope().header().rawMessage());

        // Optional: verify interactions
        Mockito.verify(mockFactory).createEnvelopeHeaderAtStart(raw, domainId);
        Mockito.verifyNoMoreInteractions(mockFactory);
    }
}
