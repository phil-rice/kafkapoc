package com.hcltech.rmg.config.enrich;

import java.util.List;
import java.util.Objects;

/**
 * Defines how to enrich a message by calling an external API using values
 * looked up from the message.
 *
 * <h2>How input values are turned into the API request</h2>
 * <ul>
 *   <li>For each message, we locate values for the configured input field paths (see {@link #inputs}).</li>
 *   <li>We take these values in order and join them with a comma (",") to form a single string.</li>
 *   <li>This comma-separated string is sent to the API (e.g., as the POST body or a query param,
 *       depending on your adapter).</li>
 * </ul>
 *
 * <p>Example intent (illustrative):</p>
 * <pre>
 * inputs: [[ "a" ], [ "b" ]]
 * output: [ "X" ]
 *
 * Incoming message:
 *   { "a": "a1", "b": "b2" }
 *
 * We extract "a1" and "b2", then send "a1,b2" to the configured {@link #url}.
 * The API responds with a value that we store under "X", producing:
 *   { "a": "a1", "b": "b2", "X": 2 }
 * </pre>
 *
 * <p>Notes:</p>
 * <ul>
 *   <li>You can declare multiple input groups (outer list). Each inner list is a set of field paths whose values are
 *       read from the message in order. The adapter will flatten in reading order and join by comma.</li>
 *   <li>{@link #output} may contain one or more field paths if the API returns a structured payload you want to fan out.</li>
 *   <li>{@link #connectTimeoutMs} and {@link #timeoutMs} are conservative blocking timeouts for the synchronous call.</li>
 * </ul>
 */
public record ApiEnrichment(
        /**
         * List of input field-path groups. Each inner list is read in order, and all resulting
         * values across groups are concatenated (with commas) to build the API request payload.
         */
        List<List<String>> inputs,

        /**
         * Field-paths to write into after a successful API call. For single-field enrichments,
         * use a singleton list like {@code List.of("X")}.
         */
        List<String> output,

        /**
         * Absolute or base URL for the API endpoint that performs the lookup/enrichment.
         */
        String url,

        /**
         * Name of the query parameter to use when sending the input values via HTTP GET.
         */
        String paramName,

        /**
         * Connection timeout in milliseconds (TCP connect).
         */
        int connectTimeoutMs,

        /**
         * Overall read timeout in milliseconds for the blocking call.
         */
        int timeoutMs
) implements EnrichmentAspect, EnrichmentWithDependencies {

    /**
     * Exposes this enrichment as its own dependency (simple self-dependency model).
     */
    @Override
    public List<EnrichmentWithDependencies> asDependencies() {
        return List.of(this);
    }

    public ApiEnrichment {
        Objects.requireNonNull(inputs, "inputs must not be null");
        Objects.requireNonNull(output, "output must not be null");
        Objects.requireNonNull(url, "url must not be null");
        Objects.requireNonNull(paramName, "paramName must not be null");

        if (inputs.isEmpty()) {
            throw new IllegalArgumentException("inputs must not be empty");
        }
        if (output.isEmpty()) {
            throw new IllegalArgumentException("output must not be empty");
        }
        if (url.isBlank()) {
            throw new IllegalArgumentException("url must not be blank");
        }
        if (paramName.isBlank()) {
            throw new IllegalArgumentException("paramName must not be blank");
        }
        if (connectTimeoutMs <= 0) {
            throw new IllegalArgumentException("connectTimeoutMs must be > 0");
        }
        if (timeoutMs <= 0) {
            throw new IllegalArgumentException("timeoutMs must be > 0");
        }
    }
}
