package com.example.cepstate;

import java.util.List;
import java.util.concurrent.CompletionStage;
import com.example.optics.IOpticsEvent;

/**
 * CEP state (event-sourced) port.
 *
 * @param <C> the concrete snapshot type
 */
public interface CepState<Optics,C> {

    /**
     * Get the current snapshot for this domain.
     * The adapter may use {@code snapshot} as a baseline/seed if no state exists yet.
     *
     * @param domainId logical key
     * @param defaultValue baseline to use if nothing stored (e.g., a default instance)
     * @return stage completing with the current state
     */
    CompletionStage<C> get(String domainId, C defaultValue);

    /**
     * Apply one or more events to this domain's state (event sourcing).
     * Events are applied in the given order; durability/ordering are the adapter's concern.
     *
     * @param domainId  logical key
     * @param mutations one or more events to apply
     * @return stage completing when the mutation is durable
     */
    @SuppressWarnings("unchecked") // varargs of a generic type by design
    CompletionStage<Void> mutate(String domainId, List<IOpticsEvent<Optics>> mutations);
}
