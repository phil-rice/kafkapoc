package com.example.cepstate.worklease;

import com.example.cepstate.IHasReason;
import org.jetbrains.annotations.Nullable;

public record AcquireResult(String reason, @Nullable String token) implements IHasReason {

    public AcquireResult(String reason) {
        this(reason, null);
    }

    public boolean isAcquired() {
        return token != null;
    }

    public boolean failed() {
        return !isAcquired();
    }
}
