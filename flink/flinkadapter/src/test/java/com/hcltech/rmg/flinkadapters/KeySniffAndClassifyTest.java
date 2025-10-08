package com.hcltech.rmg.flinkadapters;

import com.hcltech.rmg.appcontainer.impl.AppContainerFactoryForMapStringObject;
import com.hcltech.rmg.appcontainer.interfaces.AppContainer;
import com.hcltech.rmg.kafkaconfig.KafkaConfig;
import com.hcltech.rmg.messages.ErrorEnvelope;
import com.hcltech.rmg.messages.RawMessage;
import com.hcltech.rmg.messages.ValueEnvelope;
import org.codehaus.stax2.validation.XMLValidationSchema;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.ProcessFunctionTestHarnesses;
import org.apache.flink.util.OutputTag;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Locale;
import java.util.concurrent.ConcurrentLinkedQueue;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for {@link KeySniffAndClassify}.
 * Reads keyPath from AppContainerFactory.resolve("test") and generates XML accordingly.
 */
public final class KeySniffAndClassifyTest {

    // Provide TypeInformation so Flink can materialize the side output in tests
    private static final OutputTag<ErrorEnvelope< ?>> SNIFF_ERRORS =
            new OutputTag<>("sniff-errors",
                    TypeInformation.of(new TypeHint<ErrorEnvelope< ?>>() {})) {};

    // --- helpers ---------------------------------------------------------------

    private static List<String> testKeyPath() {
        AppContainer<KafkaConfig, XMLValidationSchema> c =
                AppContainerFactoryForMapStringObject.resolve("test").valueOrThrow();
        List<String> path = c.keyPath();
        assertNotNull(path, "test container keyPath() should not be null");
        assertFalse(path.isEmpty(), "test container keyPath() should not be empty");
        return path;
    }

    /** Build nested XML like: <a><b><c>TEXT</c></b></a> for path=[a,b,c]. */
    private static String xmlForPath(List<String> path, String text) {
        StringBuilder sb = new StringBuilder(64);
        for (String p : path) sb.append('<').append(p).append('>');
        sb.append(text);
        for (int i = path.size() - 1; i >= 0; i--) sb.append("</").append(path.get(i)).append('>');
        return sb.toString();
    }

    /** Replace the last segment with a different name (to simulate "missing path"). */
    private static List<String> withLastReplaced(List<String> path, String newLast) {
        java.util.ArrayList<String> copy = new java.util.ArrayList<>(path);
        copy.set(copy.size() - 1, newLast);
        return java.util.List.copyOf(copy);
    }

    private static String lower(String s) { return s == null ? "" : s.toLowerCase(Locale.ROOT); }

    // --- tests ----------------------------------------------------------------

    @Test
    @DisplayName("happy path → emits (domainId, raw) on main output")
    void happyPath_emitsTuple() throws Exception {
        var fn = new KeySniffAndClassify("test", SNIFF_ERRORS, 0);
        List<String> path = testKeyPath();
        String xml = xmlForPath(path, "ABC-123");

        try (OneInputStreamOperatorTestHarness<RawMessage, Tuple2<String, RawMessage>> h =
                     ProcessFunctionTestHarnesses.forProcessFunction(fn)) {

            h.open();

            RawMessage raw = new RawMessage(xml, 1000L, 2000L, 0, 42L, null, null, null);
            h.processElement(raw, 0);

            var out = h.extractOutputValues();
            assertEquals(1, out.size(), "Expected one main output");
            var t = out.get(0);
            assertEquals("ABC-123", t.f0);
            assertEquals(raw, t.f1);

            ConcurrentLinkedQueue<StreamRecord<ErrorEnvelope< ?>>> side = h.getSideOutput(SNIFF_ERRORS);
            assertTrue(side == null || side.isEmpty(), "No errors expected on side output");
        }
    }

    @Test
    @DisplayName("missing path → ErrorEnvelope on side output; no main output")
    void missingPath_goesToSideOutput() throws Exception {
        var fn = new KeySniffAndClassify("test", SNIFF_ERRORS, 0);
        List<String> path = testKeyPath();
        // Same tree but last element name changed so extractor won't find it
        List<String> wrongPath = withLastReplaced(path, "Other");
        String xml = xmlForPath(wrongPath, "XYZ");

        try (OneInputStreamOperatorTestHarness<RawMessage, Tuple2<String, RawMessage>> h =
                     ProcessFunctionTestHarnesses.forProcessFunction(fn)) {

            h.open();

            RawMessage raw = new RawMessage(xml, 1L, 2L, 0, 7L, null, null, null);
            h.processElement(raw, 0);

            assertTrue(h.extractOutputValues().isEmpty(), "Main output should be empty when key not found");

            var side = h.getSideOutput(SNIFF_ERRORS);
            assertNotNull(side, "Side output queue should exist");
            assertEquals(1, side.size(), "One error expected on side output");

            var err = side.iterator().next().getValue();
            assertEquals("key-sniff", err.stageName());

            ValueEnvelope<?> ve = err.envelope();
            assertNull(ve.header(), "Header should be null in sniff error");
            assertTrue(ve.data() instanceof RawMessage, "Payload should be RawMessage");
            assertEquals(raw, ve.data());

            assertFalse(err.errors().isEmpty());
            assertTrue(lower(err.errors().get(0)).contains("key"),
                    "Error message should mention key/path; got: " + err.errors().get(0));
        }
    }

    @Test
    @DisplayName("blank id text → ErrorEnvelope on side output; no main output")
    void blankId_goesToSideOutput() throws Exception {
        var fn = new KeySniffAndClassify("test", SNIFF_ERRORS, 0);
        List<String> path = testKeyPath();
        String xml = xmlForPath(path, "   "); // blank content at the leaf

        try (OneInputStreamOperatorTestHarness<RawMessage, Tuple2<String, RawMessage>> h =
                     ProcessFunctionTestHarnesses.forProcessFunction(fn)) {

            h.open();

            RawMessage raw = new RawMessage(xml, 1L, 2L, 0, 8L, null, null, null);
            h.processElement(raw, 0);

            assertTrue(h.extractOutputValues().isEmpty(), "Main output should be empty when id is blank");

            var side = h.getSideOutput(SNIFF_ERRORS);
            assertNotNull(side, "Side output queue should exist");
            assertEquals(1, side.size(), "One error expected on side output");

            var err = side.iterator().next().getValue();
            assertEquals("key-sniff", err.stageName());

            ValueEnvelope<?> ve = err.envelope();
            assertNull(ve.header());
            assertTrue(ve.data() instanceof RawMessage);
            assertEquals(raw, ve.data());

            assertFalse(err.errors().isEmpty());
            assertTrue(lower(err.errors().get(0)).contains("empty"),
                    "Error message should mention empty/blank; got: " + err.errors().get(0));
        }
    }

    @Test
    @DisplayName("multiple records → all good to main, bad to side output")
    void multipleRecords_mixGoodAndBad() throws Exception {
        var fn = new KeySniffAndClassify("test", SNIFF_ERRORS, 0);
        List<String> path = testKeyPath();

        String xmlGoodA = xmlForPath(path, "A");
        String xmlBad   = xmlForPath(withLastReplaced(path, "X"), "?");
        String xmlGoodB = xmlForPath(path, "B");

        try (OneInputStreamOperatorTestHarness<RawMessage, Tuple2<String, RawMessage>> h =
                     ProcessFunctionTestHarnesses.forProcessFunction(fn)) {

            h.open();

            var good1 = new RawMessage(xmlGoodA, 1L, 2L, 0, 1L, null, null, null);
            var bad   = new RawMessage(xmlBad,   1L, 2L, 0, 2L, null, null, null);
            var good2 = new RawMessage(xmlGoodB, 1L, 2L, 0, 3L, null, null, null);

            h.processElement(good1, 0);
            h.processElement(bad,   0);
            h.processElement(good2, 0);

            var main = h.extractOutputValues();
            assertEquals(2, main.size(), "Two good records on main output");
            assertEquals("A", main.get(0).f0);
            assertEquals("B", main.get(1).f0);

            var side = h.getSideOutput(SNIFF_ERRORS);
            assertNotNull(side);
            assertEquals(1, side.size(), "One bad record on side output");
        }
    }
}
