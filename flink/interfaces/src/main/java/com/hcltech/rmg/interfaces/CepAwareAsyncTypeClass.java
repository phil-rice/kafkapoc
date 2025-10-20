package com.hcltech.rmg.interfaces;

import com.hcltech.rmg.cepstate.CepEvent;

import java.util.List;

/**
 * CEP-aware async stage contract using a "typeclass" for completion.
 *
 * <p>Flow:</p>
 * <ol>
 *   <li><b>seed</b>: combine raw input {@code Inp} with the current CEP snapshot {@code CepState}
 *       to produce {@code Mid}. (In practice {@code Mid} is often a mutable ValueEnvelope.)</li>
 *   <li><b>asyncProcess</b>: run your (possibly multi-IO) async logic using {@code Mid}.
 *       When finished, call {@code tc.complete(sink, out)} or
 *       {@code tc.completeExceptionally(sink, error)}.</li>
 *   <li><b>mutationsOf</b>: extract the CEP event mutations from the finished {@code Out}
 *       so the operator can append them to the CEP log for this key.</li>
 * </ol>
 *
 * <p>Type parameters:</p>
 * <ul>
 *   <li>{@code Inp}  – raw input (e.g., Kafka record)</li>
 *   <li>{@code Mid}  – input enriched with CEP snapshot (often the same runtime object as Out)</li>
 *   <li>{@code Out}  – final output (e.g., ValueEnvelope after processing)</li>
 *   <li>{@code ResultFuture} – the concrete "future/sink" type your async impl uses</li>
 *   <li>{@code CepState} – CEP snapshot type</li>
 * </ul>
 *
 * <p>Rationale:</p>
 * <ul>
 *   <li>Not tied to Flink's {@code ResultFuture}; you can plug any async primitive by providing a
 *       {@link ResultFutureTC} instance.</li>
 *   <li>No per-call adapter allocations needed: pass the existing sink/future instance straight through.</li>
 * </ul>
 */
public interface CepAwareAsyncTypeClass<Inp, Mid, Out, ResultFuture, CepState> {

    /**
     * Enrich the raw input with the current CEP snapshot (pure/cheap, no I/O).
     * Often this populates a mutable envelope and returns it as {@code Mid}.
     */
    Mid addCepState(Inp in, CepState snapshot);

    /**
     * Perform the asynchronous business logic for this stage.
     *
     * <p>Implementations should start the async work (HTTP calls, DB ops, etc.) and, upon
     * completion, call exactly one of:
     * <ul>
     *   <li>{@code tc.complete(sink, out)} on success, or</li>
     *   <li>{@code tc.completeExceptionally(sink, error)} on failure.</li>
     * </ul>
     * The operator supplies both {@code tc} (how to complete) and {@code sink} (the concrete
     * future/sink instance for this invocation).</p>
     *
     * @param tc    the typeclass that knows how to complete {@code sink}
     * @param mid   input enriched with CEP snapshot
     * @param sink  the concrete future/sink for this invocation
     */
    void asyncProcess(ResultFutureTC<ResultFuture, Out> tc, Mid mid, ResultFuture sink) throws Exception;

    /**
     * Extract the CEP log mutations from the completed output.
     * Return an empty list when there are no mutations; never return null.
     */
    List<CepEvent> mutationsOf(Out out);
}
