package com.hcltech.rmg.flinkadaptersold.xml;

import com.hcltech.rmg.messages.ErrorEnvelope;
import com.hcltech.rmg.messages.RawMessage;
import com.hcltech.rmg.messages.ValueEnvelope;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.ProcessFunctionTestHarnesses;
import org.apache.flink.util.OutputTag;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;

import static org.junit.jupiter.api.Assertions.*;

final class KeySniffTest {

    private static final OutputTag<ErrorEnvelope<Object, RawMessage>> SNIFF_ERRORS =
            new OutputTag<>("sniff-errors") {};

    @Test
    @DisplayName("KeySniff: happy path → emits (domainId, raw) on main output")
    void keySniff_happyPath() throws Exception {
        var keyPath = List.of("Envelope", "Body", "Order", "Id");
        var fn = new KeySniff("test", keyPath, SNIFF_ERRORS);

        try (OneInputStreamOperatorTestHarness<RawMessage, Tuple2<String, RawMessage>> h =
                     ProcessFunctionTestHarnesses.forProcessFunction(fn)) {

            h.open();

            String xml = "<Envelope><Body><Order><Id>ABC-123</Id></Order></Body></Envelope>";
            RawMessage raw = new RawMessage(xml, 1000L, 2000L, 0, 42L, null, null, null);

            h.processElement(raw, 0);

            var out = h.extractOutputValues();
            assertEquals(1, out.size(), "Expected one main output");

            Tuple2<String, RawMessage> tuple = out.get(0);
            assertEquals("ABC-123", tuple.f0);
            assertSame(raw, tuple.f1);

            ConcurrentLinkedQueue<StreamRecord<ErrorEnvelope<Object, RawMessage>>> side =
                    h.getSideOutput(SNIFF_ERRORS);
            assertTrue(side == null || side.isEmpty(), "No errors expected on side output");
        }
    }

    @Test
    @DisplayName("KeySniff: missing path → ErrorEnvelope on side output; no main output")
    void keySniff_missingPath_toSideOutput() throws Exception {
        var keyPath = List.of("Envelope", "Body", "Order", "Id");
        var fn = new KeySniff("test", keyPath, SNIFF_ERRORS);

        try (OneInputStreamOperatorTestHarness<RawMessage, Tuple2<String, RawMessage>> h =
                     ProcessFunctionTestHarnesses.forProcessFunction(fn)) {

            h.open();

            String xml = "<Envelope><Body><Order><Other>XYZ</Other></Order></Body></Envelope>";
            RawMessage raw = new RawMessage(xml, 1L, 2L, 0, 7L, null, null, null);

            h.processElement(raw, 0);

            var out = h.extractOutputValues();
            assertTrue(out.isEmpty(), "Main output should be empty when key not found");

            ConcurrentLinkedQueue<StreamRecord<ErrorEnvelope<Object, RawMessage>>> side =
                    h.getSideOutput(SNIFF_ERRORS);
            assertNotNull(side, "Side output queue should exist");
            assertEquals(1, side.size(), "One error expected on side output");

            ErrorEnvelope<Object, RawMessage> err = side.iterator().next().getValue();
            assertEquals("key-sniff", err.stageName());
            ValueEnvelope<Object, RawMessage> ve = err.envelope();
            assertNull(ve.header());
            assertSame(raw, ve.data());
            assertFalse(err.errors().isEmpty());
            assertTrue(err.errors().get(0).toLowerCase().contains("key not found"));
        }
    }

    @Test
    @DisplayName("KeySniff: blank id text → ErrorEnvelope on side output; no main output")
    void keySniff_blankId_toSideOutput() throws Exception {
        var keyPath = List.of("root", "id");
        var fn = new KeySniff("test", keyPath, SNIFF_ERRORS);

        try (OneInputStreamOperatorTestHarness<RawMessage, Tuple2<String, RawMessage>> h =
                     ProcessFunctionTestHarnesses.forProcessFunction(fn)) {

            h.open();

            String xml = "<root><id>   </id></root>";
            RawMessage raw = new RawMessage(xml, 1L, 2L, 0, 8L, null, null, null);

            h.processElement(raw, 0);

            var out = h.extractOutputValues();
            assertTrue(out.isEmpty(), "Main output should be empty when id is blank");

            ConcurrentLinkedQueue<StreamRecord<ErrorEnvelope<Object, RawMessage>>> side =
                    h.getSideOutput(SNIFF_ERRORS);
            assertNotNull(side, "Side output queue should exist");
            assertEquals(1, side.size(), "One error expected on side output");

            ErrorEnvelope<Object, RawMessage> err = side.iterator().next().getValue();
            assertEquals("key-sniff", err.stageName());
            assertNull(err.envelope().header());
            assertSame(raw, err.envelope().data());
            assertFalse(err.errors().isEmpty());
            assertTrue(err.errors().get(0).toLowerCase().contains("empty"));
        }
    }
}
