package ai_worker.api;

public record JobStatusResponse(String jobId, boolean running) {}
