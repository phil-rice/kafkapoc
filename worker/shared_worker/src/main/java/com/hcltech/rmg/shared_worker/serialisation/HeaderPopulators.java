package com.hcltech.rmg.shared_worker.serialisation;

import com.hcltech.rmg.common.function.LFunction;
import com.hcltech.rmg.common.function.SFunction;
import com.hcltech.rmg.messages.RawMessage;
import org.apache.kafka.common.header.Headers;

import java.nio.charset.StandardCharsets;

final class HeaderPopulators {
    private HeaderPopulators() {
    }

    static void addIfPresent(Headers h, String k, String v) {
        if (v != null && !v.isEmpty()) h.add(k, v.getBytes(StandardCharsets.UTF_8));
    }

    static <E> HeaderPopulator<E> w3cTrace(SFunction<E, RawMessage> rm) {
        return (e, h) -> {
            var r = rm.apply(e);
            if (r == null) return;
            addIfPresent(h, "traceparent", r.traceparent());
            addIfPresent(h, "tracestate", r.tracestate());
            addIfPresent(h, "baggage", r.baggage());
        };
    }

    static <E> HeaderPopulator<E> domain(SFunction<E, String> domainId,
                                         LFunction<E> cepEntries,
                                         LFunction<E> duration) {
        return (e, h) -> {
            addIfPresent(h, "domainId", domainId.apply(e));
            addIfPresent(h, "cepEntries", Long.toString(cepEntries.apply(e)));
            addIfPresent(h, "duration", Long.toString(duration.apply(e)));
        };
    }

    static <E> HeaderPopulator<E> header(String name, SFunction<E, String> fn) {
        return (e, h) -> addIfPresent(h, name, fn.apply(e));
    }
}
