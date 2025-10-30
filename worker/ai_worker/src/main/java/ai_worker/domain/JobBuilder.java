package ai_worker.domain;

import com.hcltech.rmg.config.config.RootConfig;
import com.hcltech.rmg.config.configs.Configs;

import java.util.concurrent.atomic.AtomicBoolean;

// domain
public interface JobBuilder<E> {
    void build(E engine, RootConfig rootConfig, Configs configs, String condition, AtomicBoolean firstFailureAtomic) throws Exception;
}
