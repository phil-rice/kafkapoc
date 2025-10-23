package ai_worker.domain;

import java.util.Map;

/** Thrown when starting the local Flink job fails. Carries context for better error responses. */
public class JobStartException extends RuntimeException {
    private final Map<String, Object> context;

    public JobStartException(String message, Throwable cause, Map<String, Object> context) {
        super(message, cause);
        this.context = context;
    }

    public Map<String, Object> context() {
        return context;
    }
}
