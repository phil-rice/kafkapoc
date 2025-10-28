package com.hcltech.rmg.flinkadapters;

import com.hcltech.rmg.cepstate.adapter.CepEventLogAdapter;
import com.hcltech.rmg.cepstate.adapter.InMemoryCepEventLogAdapter;
import org.apache.flink.api.common.functions.RuntimeContext;

public final class FlinkCepEventLogAdapterFactory {

    private FlinkCepEventLogAdapterFactory() {}

    public static CepEventLogAdapter createAdapter(RuntimeContext ctx, String namePrefix) {
        String backend = System.getProperty("cep.state.backend");
        if (backend == null || backend.isBlank()) {
            backend = System.getenv("CEP_STATE_BACKEND");
        }
        if (backend == null || backend.isBlank()) {
            backend = "rocksdb";
        }

        switch (backend.toLowerCase()) {
            case "memory":
                return new InMemoryCepEventLogAdapter();
            case "rocksdb":
            default:
                FlinkCepEventForMapStringObjectLog log =
                        FlinkCepEventForMapStringObjectLog.from(ctx, namePrefix);
                return new FlinkCepEventLogAdapter(log);
        }
    }
}
