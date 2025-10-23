package ai_worker.domain;

import com.hcltech.rmg.config.config.RootConfig;
import com.hcltech.rmg.config.configs.Configs;

// domain
public interface JobBuilder<E> {
    void build(E engine, RootConfig rootConfig, Configs configs, String condition) throws Exception;
}
