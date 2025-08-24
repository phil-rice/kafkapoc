package com.example.cepstate.worklease;

import com.example.cepstate.IHasReason;
import org.jetbrains.annotations.Nullable;

/**
 * When we fail, we might want to retry. If we've given up we might want to run something on the backlog: that's in nextOffset.
 */
public record FailResult(String reason, @Nullable Long nextOffset, boolean willRetry, int retryCount) implements IHasReason {

    public Long nextOffset() {
        return nextOffset;
    }

    public boolean hasNextOffset() {
        return nextOffset != null;
    }
}
