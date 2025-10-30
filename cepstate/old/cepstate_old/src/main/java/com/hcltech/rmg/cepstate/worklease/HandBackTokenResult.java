package com.hcltech.rmg.cepstate.worklease;

import org.jetbrains.annotations.Nullable;
import java.io.Serializable;

public record HandBackTokenResult<Msg>(
        CompletionStatus status,
        @Nullable Msg message,
        @Nullable String token
) implements Serializable {
    public HandBackTokenResult {
        if (status == null) throw new IllegalArgumentException("status must not be null");
        switch (status) {
            case HANDED_OFF -> {
                if (message == null || token == null)
                    throw new IllegalArgumentException("HANDED_OFF requires message and token");
            }
            case ENDED, NOOP_WRONG_TOKEN, NOOP_IDLE -> {
                if (message != null || token != null)
                    throw new IllegalArgumentException(status + " must not carry message/token");
            }
        }
    }
    public static <M> HandBackTokenResult<M> handedOff(M msg, String tok) {
        return new HandBackTokenResult<>(CompletionStatus.HANDED_OFF, msg, tok);
    }
    public static <M> HandBackTokenResult<M> ended() {
        return new HandBackTokenResult<>(CompletionStatus.ENDED, null, null);
    }
    public static <M> HandBackTokenResult<M> noopWrongToken() {
        return new HandBackTokenResult<>(CompletionStatus.NOOP_WRONG_TOKEN, null, null);
    }
    public static <M> HandBackTokenResult<M> noopIdle() {
        return new HandBackTokenResult<>(CompletionStatus.NOOP_IDLE, null, null);
    }
}
