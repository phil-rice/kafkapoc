package com.hcltech.rmg.flinkadapters;

import com.hcltech.rmg.cepstate.CepEvent;
import com.hcltech.rmg.cepstate.CepEventLog;
import org.apache.flink.api.common.state.KeyedStateStore;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public final class ListStateBatchCepEventLog implements CepEventLog {
  private final ListState<List<CepEvent>> segments;

  public ListStateBatchCepEventLog(KeyedStateStore store, String name) {
    ListStateDescriptor<List<CepEvent>> desc =
        new ListStateDescriptor<>(
            name + ".segments",
            TypeInformation.of(new TypeHint<List<CepEvent>>() {}));
    this.segments = store.getListState(desc);
  }

  @Override public void append(Collection<CepEvent> batch) throws Exception {
    if (batch == null || batch.isEmpty()) return;
    segments.add(new ArrayList<>(batch)); // defensive copy
  }

  @Override public List<CepEvent> getAll() throws Exception {
    List<CepEvent> out = new ArrayList<>();
    for (List<CepEvent> seg : segments.get()) {
      if (seg != null && !seg.isEmpty()) out.addAll(seg);
    }
    return out;
  }
}
