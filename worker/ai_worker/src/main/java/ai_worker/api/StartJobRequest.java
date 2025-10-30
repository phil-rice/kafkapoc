// src/main/java/com/hcltech/rmg/ai_worker/api/StartJobRequest.java
package ai_worker.api;

import com.hcltech.rmg.config.ai.AiIncomingPayload;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;

public record StartJobRequest(
        @NotNull AiIncomingPayload incomingPayload,
        @NotBlank String condition
) {
}
