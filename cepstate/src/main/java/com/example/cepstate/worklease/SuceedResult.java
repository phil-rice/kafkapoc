package com.example.cepstate.worklease;

import com.example.cepstate.IHasReason;
import org.jetbrains.annotations.Nullable;

public record SuceedResult(String reason, @Nullable Long nextOffset) implements IHasReason {
    public Long nextOffset() {
        return nextOffset;
    }
    public boolean hasNextOffset() {
        return nextOffset != null;
    }

}
