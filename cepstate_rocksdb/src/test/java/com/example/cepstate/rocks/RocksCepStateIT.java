package com.example.cepstate.rocks;

import com.example.kafka.common.Codec;
import com.example.optics.IOpticsEvent;
import com.example.optics.Interpreter;
import org.apache.commons.jxpath.JXPathContext;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.InOrder;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/**
 * Integration tests for RocksCepState (JXPath flavor):
 *  - real RocksDB
 *  - name-based codec mapping "event1" -> IOpticsEvent<JXPathContext> instance
 *  - mocked Interpreter<JXPathContext, TestState>
 */
public class RocksCepStateIT {

    static { RocksDB.loadLibrary(); }

    @TempDir Path dbDir;

    private RocksDB db;
    private Interpreter<JXPathContext, TestState> interpreter; // mocked
    private Codec<List<IOpticsEvent<JXPathContext>>, byte[]> codec;
    private RocksCepState<JXPathContext, TestState> sut;
    private Map<String, IOpticsEvent<JXPathContext>> nameToEvent;

    @BeforeEach
    void setUp() throws Exception {
        try (Options options = new Options().setCreateIfMissing(true)) {
            db = RocksDB.open(options, dbDir.toString());
        }
        nameToEvent = buildEvents();
        codec = new NamedEventCodec(nameToEvent);
        interpreter = mock(Interpreter.class);
        sut = new RocksCepState<>(db, codec, interpreter);
    }

    @AfterEach
    void tearDown() {
        if (db != null) db.close();
    }

    // -------------------- Tests --------------------

    @Test
    void get_onEmptyDomain_returnsDefault_liftsAndReturns_withoutApply() throws Exception {
        TestState def = new TestState("x", 0, "y");
        JXPathContext m0 = ctx();

        when(interpreter.lift(def)).thenReturn(m0);
        when(interpreter.getFrom(m0)).thenReturn(def);

        TestState out = sut.get("domain-empty", def).toCompletableFuture().get(2, TimeUnit.SECONDS);

        assertEquals(def, out);
        verify(interpreter, times(1)).lift(def);
        verify(interpreter, never()).apply(anyList(), any());
        verify(interpreter, times(1)).getFrom(m0);
        verifyNoMoreInteractions(interpreter);
    }

    @Test
    void mutate_thenGet_singleBatch_callsInterpreterOnce_withDecodedEvents() throws Exception {
        String domain = "d1";
        List<IOpticsEvent<JXPathContext>> batch = List.of(
                nameToEvent.get("event1"), // a = "A1"
                nameToEvent.get("event2")  // b = 1
        );

        TestState def = new TestState(null, null, null);
        TestState after = new TestState("A1", 1, null);
        JXPathContext m0 = ctx();
        JXPathContext m1 = ctx();

        when(interpreter.lift(def)).thenReturn(m0);
        when(interpreter.apply(eq(batch), eq(m0))).thenReturn(m1);
        when(interpreter.getFrom(m1)).thenReturn(after);

        sut.mutate(domain, batch).toCompletableFuture().get(2, TimeUnit.SECONDS);

        TestState out = sut.get(domain, def).toCompletableFuture().get(2, TimeUnit.SECONDS);
        assertEquals(after, out);

        verify(interpreter, times(1)).lift(def);
        verify(interpreter, times(1)).apply(eq(batch), eq(m0));
        verify(interpreter, times(1)).getFrom(m1);
        verifyNoMoreInteractions(interpreter);
    }

    @Test
    void mutate_multipleBatches_foldedInOrder() throws Exception {
        String domain = "d-multi";
        List<IOpticsEvent<JXPathContext>> batch1 = List.of(nameToEvent.get("event1")); // a = "A1"
        List<IOpticsEvent<JXPathContext>> batch2 = List.of(nameToEvent.get("event3"), nameToEvent.get("event5")); // c = "C3", b = 5

        TestState def = new TestState(null, null, null);
        TestState s2 = new TestState("A1", 5, "C3");
        JXPathContext m0 = ctx();
        JXPathContext m1 = ctx();
        JXPathContext m2 = ctx();

        when(interpreter.lift(def)).thenReturn(m0);
        when(interpreter.apply(eq(batch1), eq(m0))).thenReturn(m1);
        when(interpreter.apply(eq(batch2), eq(m1))).thenReturn(m2);
        when(interpreter.getFrom(m2)).thenReturn(s2);

        sut.mutate(domain, batch1).toCompletableFuture().get(2, TimeUnit.SECONDS);
        sut.mutate(domain, batch2).toCompletableFuture().get(2, TimeUnit.SECONDS);

        TestState out = sut.get(domain, def).toCompletableFuture().get(2, TimeUnit.SECONDS);
        assertEquals(s2, out);

        InOrder inOrder = inOrder(interpreter);
        inOrder.verify(interpreter).lift(def);
        inOrder.verify(interpreter).apply(eq(batch1), eq(m0));
        inOrder.verify(interpreter).apply(eq(batch2), eq(m1));
        inOrder.verify(interpreter).getFrom(m2);
        verifyNoMoreInteractions(interpreter);
    }
    @Test
    void domainIsolation_iteratesOnlyWithinDomainRange() throws Exception {
        String A = "tenantA";
        String B = "tenantB";

        List<IOpticsEvent<JXPathContext>> batchA = List.of(nameToEvent.get("event2")); // b = 1
        List<IOpticsEvent<JXPathContext>> batchB = List.of(nameToEvent.get("event4")); // a = "A4"

        TestState def = new TestState(null, null, null);
        TestState aOut = new TestState(null, 1, null);
        TestState bOut = new TestState("A4", null, null);

        // distinct "Main" tokens for each get
        JXPathContext mA0 = ctx(), mA1 = ctx();
        JXPathContext mB0 = ctx(), mB1 = ctx();

        // When lifting the default state for each domain
        when(interpreter.lift(def)).thenReturn(mA0, mB0);
        // Apply events to their lifted states
        when(interpreter.apply(eq(batchA), eq(mA0))).thenReturn(mA1);
        when(interpreter.apply(eq(batchB), eq(mB0))).thenReturn(mB1);
        // And convert back to concrete state
        when(interpreter.getFrom(mA1)).thenReturn(aOut);
        when(interpreter.getFrom(mB1)).thenReturn(bOut);

        sut.mutate(A, batchA).toCompletableFuture().get(2, TimeUnit.SECONDS);
        sut.mutate(B, batchB).toCompletableFuture().get(2, TimeUnit.SECONDS);

        TestState readA = sut.get(A, def).toCompletableFuture().get(2, TimeUnit.SECONDS);
        TestState readB = sut.get(B, def).toCompletableFuture().get(2, TimeUnit.SECONDS);

        assertEquals(aOut, readA);
        assertEquals(bOut, readB);

        InOrder inOrder = inOrder(interpreter);
        inOrder.verify(interpreter).lift(def);
        inOrder.verify(interpreter).apply(eq(batchA), eq(mA0));
        inOrder.verify(interpreter).getFrom(mA1);
        inOrder.verify(interpreter).lift(def);
        inOrder.verify(interpreter).apply(eq(batchB), eq(mB0));
        inOrder.verify(interpreter).getFrom(mB1);

        verifyNoMoreInteractions(interpreter);
    }


    @Test
    void seqIsPersistedAndMonotonic_perDomain() throws Exception {
        String d = "seqD";
        List<IOpticsEvent<JXPathContext>> b1 = List.of(nameToEvent.get("event1"));
        List<IOpticsEvent<JXPathContext>> b2 = List.of(nameToEvent.get("event3"));

        // interpreter not used by mutate(); no need to stub apply

        sut.mutate(d, b1).toCompletableFuture().get(2, TimeUnit.SECONDS);
        sut.mutate(d, b2).toCompletableFuture().get(2, TimeUnit.SECONDS);

        byte[] seqKey = seqKey(d);
        byte[] raw = db.get(seqKey);
        assertNotNull(raw, "seq:<id> key should exist");
        long last = ByteBuffer.wrap(raw).getLong();
        assertEquals(1L, last, "after two appends (0-based), last seq should be 1");
    }

    @Test
    void mutate_emptyList_isNoOp_doesNotAdvanceSeq() throws Exception {
        String d = "noop";
        sut.mutate(d, List.of()).toCompletableFuture().get(2, TimeUnit.SECONDS);

        assertNull(db.get(seqKey(d)), "empty mutation should not create seq:<id> key");
        verifyNoInteractions(interpreter);
    }

    // -------------------- Helpers --------------------

    static record TestState(String a, Integer b, String c) {}

    private static Map<String, IOpticsEvent<JXPathContext>> buildEvents() {
        Map<String, IOpticsEvent<JXPathContext>> m = new LinkedHashMap<>();
        // Paths are illustrative; we don't execute them in these tests
        m.put("event1", IOpticsEvent.setEvent("a", "A1"));
        m.put("event2", IOpticsEvent.setEvent("b", 1));
        m.put("event3", IOpticsEvent.setEvent("c", "C3"));
        m.put("event4", IOpticsEvent.setEvent("a", "A4"));
        m.put("event5", IOpticsEvent.setEvent("b", 5));
        return m;
    }

    /** Name-based codec using identity of event instances (round-trips exact objects). */
    static final class NamedEventCodec implements Codec<List<IOpticsEvent<JXPathContext>>, byte[]> {
        private final Map<String, IOpticsEvent<JXPathContext>> nameToEvent;
        private final IdentityHashMap<IOpticsEvent<JXPathContext>, String> eventToName;

        NamedEventCodec(Map<String, IOpticsEvent<JXPathContext>> nameToEvent) {
            this.nameToEvent = Map.copyOf(nameToEvent);
            this.eventToName = new IdentityHashMap<>();
            this.nameToEvent.forEach((k, v) -> this.eventToName.put(v, k));
        }

        @Override
        public byte[] encode(List<IOpticsEvent<JXPathContext>> value) {
            String joined = value.stream()
                    .map(e -> {
                        String name = eventToName.get(e);
                        if (name == null) throw new IllegalArgumentException("Unknown event instance: " + e);
                        return name;
                    })
                    .collect(Collectors.joining("\n"));
            return joined.getBytes(StandardCharsets.UTF_8);
        }

        @Override
        public List<IOpticsEvent<JXPathContext>> decode(byte[] bytes) {
            if (bytes == null || bytes.length == 0) return List.of();
            String s = new String(bytes, StandardCharsets.UTF_8);
            List<IOpticsEvent<JXPathContext>> out = new ArrayList<>();
            for (String token : s.split("\n", -1)) {
                if (token.isEmpty()) continue;
                IOpticsEvent<JXPathContext> ev = nameToEvent.get(token);
                if (ev == null) throw new IllegalArgumentException("Unknown event name: " + token);
                out.add(ev);
            }
            return List.copyOf(out);
        }
    }

    private static byte[] seqKey(String domainId) {
        byte[] NS_SEQ = "seq:".getBytes(StandardCharsets.UTF_8);
        byte[] id = domainId.getBytes(StandardCharsets.UTF_8);
        ByteBuffer buf = ByteBuffer.allocate(NS_SEQ.length + 4 + id.length);
        buf.put(NS_SEQ).putInt(id.length).put(id);
        return buf.array();
    }

    private static JXPathContext ctx() {
        return JXPathContext.newContext(new Object());
    }
}
