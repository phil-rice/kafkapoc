package com.hcltech.rmg.cepstate.worklease;

import com.hcltech.rmg.cepstate.IHasReason;
import org.jetbrains.annotations.Nullable;

public record SuceedResult(String reason, @Nullable Long nextOffset) implements IHasReason {
    public Long nextOffset() {
        return nextOffset;
    }
    public boolean hasNextOffset() {
        return nextOffset != null;
    }

}
