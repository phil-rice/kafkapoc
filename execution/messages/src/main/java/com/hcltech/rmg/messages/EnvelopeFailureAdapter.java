package com.hcltech.rmg.messages;

import com.hcltech.rmg.common.async.FailureAdapter;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Maps failures/timeouts on {@link Envelope} to domain envelopes.
 * - Builds a fresh {@link ValueEnvelope} (no aliasing the input VE).
 * - Preserves header/data/cepState; does NOT touch seq (async executor tags it).
 * - Failures -> {@link ErrorEnvelope}; Timeouts -> {@link RetryEnvelope} (by design).
 */
public final class EnvelopeFailureAdapter<CepState, Msg>
        implements FailureAdapter<Envelope<CepState, Msg>, Envelope<CepState, Msg>>, Serializable {

    private final String operatorId;

    public EnvelopeFailureAdapter(String operatorId) {
        this.operatorId = Objects.requireNonNull(operatorId, "operatorId");
    }

    /** Factory: default failure adapter for Envelope. */
    public static <S, M> FailureAdapter<Envelope<S, M>, Envelope<S, M>> defaultAdapter(String operatorId) {
        return new EnvelopeFailureAdapter<>(operatorId);
    }

    @Override
    public Envelope<CepState, Msg> onFailure(Envelope<CepState, Msg> in, Throwable error) {
        Objects.requireNonNull(in, "in");
        Objects.requireNonNull(error, "error");

        var inVE = in.valueEnvelope();
        var outVE = new ValueEnvelope<>(
                inVE.header(),               // preserve header (domainId, params, etc.)
                inVE.data(),                 // preserve payload
                inVE.cepState(),             // preserve cep snapshot
                new ArrayList<>()            // empty modifications on failure
        );

        List<String> errors = compactErrors(error);
        return new ErrorEnvelope<>(outVE, operatorId, errors);
    }

    @Override
    public Envelope<CepState, Msg> onTimeout(Envelope<CepState, Msg> in, long elapsedNanos) {
        Objects.requireNonNull(in, "in");

        var inVE = in.valueEnvelope();
        var outVE = new ValueEnvelope<>(
                inVE.header(),
                inVE.data(),
                inVE.cepState(),
                new ArrayList<>()            // empty modifications on timeout
        );

        // Design choice: emit a RetryEnvelope (not ErrorEnvelope).
        // If you prefer error propagation instead of retry, swap to ErrorEnvelope with a message.
        return new RetryEnvelope<>(outVE, operatorId, 0 /*initial backoff*/);
    }

    // --- helpers ---

    private static List<String> compactErrors(Throwable t) {
        List<String> msgs = new ArrayList<>(4);
        if (t == null) return msgs; // defensive; callers already check non-null

        int depth = 0;
        for (Throwable cur = t; cur != null && depth < 3; cur = cur.getCause(), depth++) {
            String cls = cur.getClass().getSimpleName();
            String msg = cur.getMessage();
            msgs.add(msg == null ? cls : (cls + ": " + msg));
        }
        // add ellipsis if chain is longer than what we captured
        if (t.getCause() != null && t.getCause().getCause() != null) {
            msgs.add("...");
        }
        return msgs;
    }
}
