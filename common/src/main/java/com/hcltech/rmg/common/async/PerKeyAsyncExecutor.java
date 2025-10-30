package com.hcltech.rmg.common.async;

/** Per-key async executor. Operator calls execute(), onWake(), needsWake(), finish(). */
public interface PerKeyAsyncExecutor<Inp, Out> {
    AcceptResult execute(Inp input);
    void onWake();           // drain completions, deliver in-order, launch from pending as capacity allows
    boolean needsWake();     // true if inbox or inFlight>0 (schedule/reschedule an immediate timer)
    void finish();           // stop accepting; ignore late completions
}
