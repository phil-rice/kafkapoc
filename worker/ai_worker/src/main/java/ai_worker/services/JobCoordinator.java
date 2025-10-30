// src/main/java/com/hcltech/rmg/ai_worker/app/JobCoordinator.java
package ai_worker.services;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import com.hcltech.rmg.config.config.RootConfig;
import com.hcltech.rmg.config.configs.Configs;
import com.hcltech.rmg.flinkadapters.PerfStats;
import com.hcltech.rmg.shared_worker.FirstHitJobKiller;
import org.springframework.stereotype.Service;
import ai_worker.domain.FlinkJobPort;

@Service
public class JobCoordinator {
    private final FlinkJobPort flink;
    private final AtomicReference<String> currentJobId = new AtomicReference<>(null);

    public JobCoordinator(FlinkJobPort flink) {
        this.flink = flink;
    }

    public String restartWith(RootConfig rootConfig, Configs configs, String condition) {
        String existing = currentJobId.getAndSet(null);
        if (existing != null) {
            try {
                flink.killJob(existing);
            } catch (Exception ignored) {
            }
        }
        var firstFailAtomic = new AtomicBoolean(false);
        String id = flink.startJob(rootConfig, configs, condition, firstFailAtomic);
        currentJobId.set(id);
        flink.setUpFirstFailureJobKiller(id, firstFailAtomic);
        return id;
    }

    public boolean isRunning(String jobId) {
        String curr = currentJobId.get();
        return curr != null && curr.equals(jobId);
    }

    public void kill(String jobId) {
        try {
            flink.killJob(jobId);
            PerfStats.clear();
        } finally {
            currentJobId.compareAndSet(jobId, null);
        }
    }

    public String currentJobId() {
        return currentJobId.get();
    }
}
