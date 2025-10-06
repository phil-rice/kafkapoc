package com.hcltech.rmg.cepstate;

import com.hcltech.rmg.flinkadapters.FlinkCepEventLog;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** Operator mirroring production usage of FlinkCepEventLog. */
public final class CepEventLogOp extends RichFlatMapFunction<CepEventLogOp.In, CepEventLogOp.Out> {

    // --- Inputs ---
    public interface In {}
    public static final class Append implements In {
        private final String domainId;
        private final List<CepEvent> batch;
        public Append(String domainId, List<CepEvent> batch) { this.domainId = domainId; this.batch = batch; }
        public String domainId() { return domainId; }
        public List<CepEvent> batch() { return batch; }
    }
    public static final class QueryAll implements In {
        private final String domainId;
        public QueryAll(String domainId) { this.domainId = domainId; }
        public String domainId() { return domainId; }
    }
    public static final class QueryFold implements In {
        private final String domainId;
        public QueryFold(String domainId) { this.domainId = domainId; }
        public String domainId() { return domainId; }
    }

    // --- Outputs ---
    public interface Out {}
    public static final class Ack implements Out {
        private final String domainId;
        private final int appended;
        public Ack(String domainId, int appended) { this.domainId = domainId; this.appended = appended; }
        public String domainId() { return domainId; }
        public int appended() { return appended; }
    }
    public static final class AllEvents implements Out {
        private final String domainId;
        private final List<CepEvent> events;
        public AllEvents(String domainId, List<CepEvent> events) { this.domainId = domainId; this.events = events; }
        public String domainId() { return domainId; }
        public List<CepEvent> events() { return events; }
    }
    public static final class FoldedState implements Out {
        private final String domainId;
        private final Map<String,Object> state;
        public FoldedState(String domainId, Map<String,Object> state) { this.domainId = domainId; this.state = state; }
        public String domainId() { return domainId; }
        public Map<String,Object> state() { return state; }
    }

    private transient CepEventLog log;

    @Override public void open(OpenContext ctx) { this.log = FlinkCepEventLog.from(getRuntimeContext(), "cepLog"); }

    @Override
    public void flatMap(In in, Collector<Out> out) throws Exception {
        if (in instanceof Append a) {
            int n = (a.batch() == null) ? 0 : a.batch().size();
            if (n > 0) log.append(a.batch());
            out.collect(new Ack(a.domainId(), n));
        } else if (in instanceof QueryAll q) {
            out.collect(new AllEvents(q.domainId(), log.getAll()));
        } else if (in instanceof QueryFold qf) {
            var state = new HashMap<String,Object>();
            out.collect(new FoldedState(qf.domainId(), log.foldAll(state)));
        }
    }
}
