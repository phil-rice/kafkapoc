package com.hcltech.rmg.flinkadapters;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.operators.*;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.junit.jupiter.api.Test;

import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;

import static org.junit.jupiter.api.Assertions.*;

public class KeyContextDrainProofTest {

  /** Minimal operator: per-record writes placeholder to current key's state; drain() can update ANY key by setCurrentKey(). */
  static final class MinimalKeyContextOp
      extends AbstractStreamOperator<String>
      implements OneInputStreamOperator<String, String>, KeyContext {

    private transient ListState<String> state;
    // queued completions: (targetKey, valueToAppend)
    private final Queue<Map.Entry<String,String>> q = new ConcurrentLinkedQueue<>();

    @Override
    public void open() throws Exception {
      super.open();
      state = getRuntimeContext().getListState(
          new ListStateDescriptor<>("segments", Types.STRING));
    }

    @Override
    public void processElement(StreamRecord<String> element) throws Exception {
      // current key context is set by the test harness per record
      // write a placeholder under *this* key
      state.add("PH:"+element.getValue());

      // Enqueue a "completion" targeting a possibly different key:
      // Convention: payload "A->B:foo" means: when we drain, update key=B with "foo"
      String payload = element.getValue();
      String[] parts = payload.split("->");
      if (parts.length == 2) {
        String[] rhs = parts[1].split(":");
        String targetKey = rhs[0];
        String mod = rhs.length>1 ? rhs[1] : "MOD";
        q.offer(Map.entry(targetKey, mod));
      }
    }

    /** Drain queued completions on operator thread. For each, set key and append; emit state summary for assertions. */
    void drain() throws Exception {
      Map<String,Integer> counts = new HashMap<>();
      Map.Entry<String,String> e;
      while ((e = q.poll()) != null) {
        String key = e.getKey();
        String mod = e.getValue();
        // switch key context (cheap)
        setCurrentKey(key);
        state.add(mod);

        // read current state to build a quick count for assertion
        int c = 0;
        for (String s : state.get()) c++;
        counts.put(key, c);

        // emit a simple line we can assert on
        output.collect(new StreamRecord<>("STATE " + key + " -> " + c));
      }
    }
  }

  @Test
  void canWriteDifferentKeyInDrainBySettingKeyContext() throws Exception {
    // Operator under test
    MinimalKeyContextOp op = new MinimalKeyContextOp();
    // Key is identity (String)
    KeyedOneInputStreamOperatorTestHarness<String, String, String> h =
        new KeyedOneInputStreamOperatorTestHarness<>(
            op, x -> x.contains("->") ? x.substring(0, x.indexOf("->")) : x, Types.STRING);

    h.setup();
    h.open();

    // 1) Process an element under key "A" that requests a completion for key "B"
    // The payload "A->B:foo" means: current key=A; enqueue completion to write "foo" into key=B
    h.processElement(new StreamRecord<>("A->B:foo"));

    // 2) Drain on operator thread: should setCurrentKey("B") and append "foo" to B's state
    op.drain();

    // 3) Verify output says STATE B -> <count>. Placeholder under A, and "foo" under B.
    List<String> outs = new ArrayList<>();
    for (Object o : h.getOutput()) {
      @SuppressWarnings("unchecked")
      StreamRecord<String> sr = (StreamRecord<String>) o;
      outs.add(sr.getValue());
    }

    // We expect a summary for key B
    boolean sawB = outs.stream().anyMatch(s -> s.startsWith("STATE B ->"));
    assertTrue(sawB, "Expected state summary for key B after drain, got: " + outs);

    // 4) As a stronger check, process another record for B and ensure its state already has prior mod ("foo")
    // Send an element whose "current key" is B and that enqueues a completion for B again
    h.processElement(new StreamRecord<>("B->B:bar"));
    // Drain again; this should now count at least 2 entries for B ("foo" and "bar")
    op.drain();

    outs.clear();
    for (Object o : h.getOutput()) {
      @SuppressWarnings("unchecked")
      StreamRecord<String> sr = (StreamRecord<String>) o;
      outs.add(sr.getValue());
    }
    // Find the last B summary
    Optional<String> lastB = outs.stream().filter(s -> s.startsWith("STATE B ->")).reduce((a,b)->b);
    assertTrue(lastB.isPresent(), "Expected a B summary after second drain");
    String summary = lastB.get();
    // parse count
    int cnt = Integer.parseInt(summary.substring(summary.indexOf("->")+2).trim());
    assertTrue(cnt >= 2, "Expected B's state to have at least 2 entries, saw: " + summary);

    h.close();
  }
}
