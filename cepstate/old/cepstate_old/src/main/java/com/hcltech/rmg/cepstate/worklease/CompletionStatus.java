package com.hcltech.rmg.cepstate.worklease;

public enum CompletionStatus {
    HANDED_OFF,       // dequeued next and advanced epoch
    ENDED,            // queue empty; lease cleared
    NOOP_WRONG_TOKEN, // stale/incorrect token; state unchanged
    NOOP_IDLE         // no lease exists (already cleared)
}