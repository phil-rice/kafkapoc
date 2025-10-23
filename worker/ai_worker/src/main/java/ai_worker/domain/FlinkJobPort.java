// src/main/java/com/hcltech/rmg/ai_worker/domain/FlinkJobPort.java
package ai_worker.domain;

import com.hcltech.rmg.config.config.RootConfig;
import com.hcltech.rmg.config.configs.Configs;

import java.util.concurrent.atomic.AtomicBoolean;

public interface FlinkJobPort {
    String startJob(RootConfig rootConfig, Configs configs, String condition, AtomicBoolean firstFailureAtomic);

    void setUpFirstFailureJobKiller(String jobId,AtomicBoolean firstFailureAtomic);

    void killJob(String jobId);
}
