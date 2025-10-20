package com.hcltech.rmg.common.async;

/**
 * Typeclass/port for per-envelope correlation and stable lane routing.
 * - correlationId must be unique per event (for tracing/metrics).
 * - laneHash must be a stable hash for routing; it need not equal correlationId.
 */
public interface Correlator<Env> {
    long correlationId(Env env);
    int  laneHash(Env env); // laneIndex = laneHash(env) & (lanes - 1)
}
