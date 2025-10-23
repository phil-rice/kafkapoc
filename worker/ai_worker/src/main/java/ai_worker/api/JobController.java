// src/main/java/com/hcltech/rmg/ai_worker/api/JobController.java
package ai_worker.api;

import com.hcltech.rmg.config.ai.AiIncomingPayload;
import com.hcltech.rmg.config.ai.AiPayloadToConfigs;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import ai_worker.services.JobCoordinator;

@RestController
@RequestMapping("/jobs")
public class JobController {
    private final JobCoordinator coordinator;

    public JobController(JobCoordinator coordinator) {
        this.coordinator = coordinator;
    }

    @PostMapping
    public ResponseEntity<StartJobResponse> start(
            @RequestBody @NotNull AiIncomingPayload body
    ) {
        var currentJob = coordinator.currentJobId();
        if (currentJob != null) coordinator.kill(currentJob);
        var configs = AiPayloadToConfigs.toConfigs(body);
        String id = coordinator.restartWith(body.rootConfig(), configs, body.celProjection());
        return ResponseEntity.status(HttpStatus.ACCEPTED).body(new StartJobResponse(id));
    }


    @GetMapping("/{jobId}/status")
    public JobStatusResponse status(@PathVariable String jobId) {
        return new JobStatusResponse(jobId, coordinator.isRunning(jobId));
    }

    @PostMapping("/{jobId}/kill")
    public ResponseEntity<Void> kill(@PathVariable String jobId) {
        coordinator.kill(jobId);
        return ResponseEntity.noContent().build();
    }

    @GetMapping("/current/status")
    public JobStatusResponse currentStatus() {
        String id = coordinator.currentJobId();
        return new JobStatusResponse(id, id != null);
    }
}
