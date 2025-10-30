package com.hcltech.rmg.enrichment;

import com.hcltech.rmg.cepstate.CepStateTypeClass;
import com.hcltech.rmg.messages.MsgTypeClass;
import com.hcltech.rmg.messages.ValueEnvelope;

import java.util.AbstractList;
import java.util.List;
import java.util.Objects;

public final class EnricherHelper {

    private EnricherHelper() {}

    private static final String ROOT_INP = "inp";
    private static final String ROOT_CEP = "cep";

    /**
     * Builds a composite key by reading configured inputs from either the message payload (inp)
     * or CEP state (cep) and joining with the provided delimiter.
     *
     * Returns null if:
     *  - any input path is null/empty, or
     *  - "cep" is requested but cepState is null, or
     *  - any resolved component value is null.
     */
    public static <CepState, Msg> String buildCompositeKey(
            List<List<String>> inputs,
            String keyDelimiter,
            ValueEnvelope<CepState, Msg> envelope,
            MsgTypeClass<Msg, List<String>> msgTypeClass,
            CepStateTypeClass<CepState> cepStateTypeClass
    ) {
        // Nulls -> NPE (matches your preference and existing tests)
        Objects.requireNonNull(inputs, "inputs");
        Objects.requireNonNull(keyDelimiter, "keyDelimiter");
        Objects.requireNonNull(envelope, "envelope");
        Objects.requireNonNull(msgTypeClass, "msgTypeClass");
        Objects.requireNonNull(cepStateTypeClass, "cepStateTypeClass");

        final StringBuilder sb = new StringBuilder();

        for (List<String> rawPath : inputs) {
            if (rawPath == null || rawPath.isEmpty()) {
                return null; // no root selector provided -> treat as missing
            }

            final String root = rawPath.get(0);

            final Object value;
            if (ROOT_INP.equals(root)) {
                // Read from message payload via MsgTypeClass; avoid subList
                value = msgTypeClass.getValueFromPath(
                        envelope.data(),
                        rawPath.size() > 1 ? new OffsetList(rawPath, 1) : List.of()
                );

            } else if (ROOT_CEP.equals(root)) {
                final CepState state = envelope.cepState();
                if (state == null) {
                    return null; // requested CEP but no state available
                }
                value = cepStateTypeClass.getFromPath(
                        state,
                        rawPath.size() > 1 ? new OffsetList(rawPath, 1) : List.of()
                );

            } else {
                // Legacy fallback: treat the whole path as data path
                value = msgTypeClass.getValueFromPath(envelope.data(), rawPath);
            }

            if (value == null) {
                return null; // missing component -> abort enrichment
            }

            if (sb.length() > 0) sb.append(keyDelimiter);
            sb.append(String.valueOf(value));
        }

        return sb.toString();
    }

    /** Lightweight, read-only view over a List<String> starting at an offset (no copying). */
    private static final class OffsetList extends AbstractList<String> {
        private final List<String> backing;
        private final int offset;
        private final int size;

        OffsetList(List<String> backing, int offset) {
            if (backing == null) throw new NullPointerException("backing");
            if (offset < 0 || offset > backing.size()) {
                throw new IndexOutOfBoundsException("offset " + offset + " for size " + backing.size());
            }
            this.backing = backing;
            this.offset = offset;
            this.size = backing.size() - offset;
        }

        @Override public String get(int index) {
            if (index < 0 || index >= size) throw new IndexOutOfBoundsException(index);
            return backing.get(offset + index);
        }

        @Override public int size() {
            return size;
        }
    }
}
