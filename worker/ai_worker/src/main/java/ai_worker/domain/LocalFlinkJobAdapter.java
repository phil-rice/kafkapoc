package ai_worker.domain;

import com.hcltech.rmg.config.config.RootConfig;
import com.hcltech.rmg.config.configs.Configs;
import com.hcltech.rmg.flinkadapters.FlinkHelper;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.springframework.stereotype.Component;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

@Component
public class LocalFlinkJobAdapter implements FlinkJobPort {

    private final JobBuilder<StreamExecutionEnvironment> builder;
    private final Map<String, JobClient> running = new ConcurrentHashMap<>();

    public LocalFlinkJobAdapter(JobBuilder<StreamExecutionEnvironment> builder) {
        this.builder = builder;
    }

    @Override
    public String startJob(RootConfig rootConfig, Configs configs, String condition) {
        try {
            Configuration conf = FlinkHelper.makeDefaultFlinkConfig();
            StreamExecutionEnvironment env =
                    StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);
            env.getConfig().setClosureCleanerLevel(ExecutionConfig.ClosureCleanerLevel.NONE);
            // Build pipeline (no execute here)
            builder.build(env, rootConfig, configs, condition);

            JobClient client = env.executeAsync("ai-worker-job");
            JobID jobID = client.getJobID();
            String id = jobID.toHexString();
            running.put(id, client);
            return id;
        } catch (Exception e) {
            // Collect some context without dumping the whole config (trim sensitive data)
            Map<String, Object> ctx = new LinkedHashMap<>();
            ctx.put("phase", "executeAsync");
            ctx.put("condition", condition);
            // Light summary of config (avoid huge payloads)
            try {
                var keys = configs.keyToConfigMap().keySet();
                ctx.put("config keys", keys);
            } catch (Exception ignored) {
            }
            throw new JobStartException("Failed to start local Flink job", e, ctx);
        }
    }

    @Override
    public void killJob(String jobId) {
        JobClient client = running.remove(jobId);
        if (client == null) return; // idempotent
        try {
            client.cancel().get(5, TimeUnit.SECONDS);
        } catch (Exception e) {
            // Best-effort cancel; log if you like
            System.err.println("Failed to cancel Flink job " + jobId + ": " + e.getMessage());
        }
    }
}
