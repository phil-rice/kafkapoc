package com.hcltech.rmg.flinkadapters;

import com.hcltech.rmg.cepstate.CepEvent;
import com.hcltech.rmg.cepstate.CepEventLog;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.util.ArrayList;
import java.util.List;

public final class FlinkCepEventLog implements CepEventLog {

    private final ListState<List<CepEvent>> segments;

    public static FlinkCepEventLog from(RuntimeContext ctx, String namePrefix) {
        return new FlinkCepEventLog(ctx, namePrefix);
    }

    private FlinkCepEventLog(RuntimeContext ctx, String namePrefix) {
        ListStateDescriptor<List<CepEvent>> desc = new ListStateDescriptor<>(
                namePrefix + ".segments",
                TypeInformation.of(new TypeHint<List<CepEvent>>() {})
        );
        this.segments = ctx.getListState(desc);
    }

    @Override
    public void append(List<CepEvent> batch) throws Exception {
        if (batch == null || batch.isEmpty()) return;
        // Defensive copy so callers can’t mutate after append
        segments.add(new ArrayList<>(batch));
    }

    @Override
    public List<CepEvent> getAll() throws Exception {
        List<CepEvent> out = new ArrayList<>();
        for (List<CepEvent> seg : segments.get()) {
            if (seg != null && !seg.isEmpty()) out.addAll(seg);
        }
        return out;
    }
}
