package com.hcltech.rmg.common.async;

import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.CompletionStage;
import java.util.function.BiConsumer;
import java.util.function.Function;

/** Backpressure signal from execute(...) */
public enum AcceptResult {
    LAUNCHED,      // started async immediately (inFlight++)
    QUEUED,        // queued in pending (bounded ≤ K)
    REJECTED_FULL  // neither launched nor queued; caller should backpressure
}

