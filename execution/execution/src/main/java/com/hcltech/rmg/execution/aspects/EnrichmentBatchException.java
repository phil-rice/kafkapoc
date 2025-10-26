package com.hcltech.rmg.execution.aspects;

import java.util.Collections;
import java.util.List;

/** Aggregated failures for a generation. Forward via your continuation; do not catch. */
public final class EnrichmentBatchException extends RuntimeException {
    /** Immutable, human-readable summary per failed slot. */
    public static final class ErrorInfo {
        private final int slotIndex;
        private final String component;   // pre-stringified to avoid retaining config objects
        private final String message;     // error.getMessage()
        private final Throwable cause;    // original throwable, for logs

        public ErrorInfo(int slotIndex, String component, Throwable cause) {
            this.slotIndex = slotIndex;
            this.component = component;
            this.cause = cause;
            this.message = (cause == null ? null : cause.getMessage());
        }
        public int slotIndex() { return slotIndex; }
        public String component() { return component; }
        public String message() { return message; }
        public Throwable cause() { return cause; }
        @Override public String toString() {
            return "slot=" + slotIndex + ", component=" + component + ", error=" + message;
        }
    }

    private final int generationIndex;
    private final List<ErrorInfo> errors;  // small list, immutable

    public EnrichmentBatchException(int generationIndex, List<ErrorInfo> errors) {
        super("Generation " + generationIndex + " failed with " + errors.size() + " error(s)\n" + errors);
        this.generationIndex = generationIndex;
        this.errors = Collections.unmodifiableList(errors);
    }

    public int generationIndex() { return generationIndex; }
    public List<ErrorInfo> errors() { return errors; }
}
