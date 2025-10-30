package com.hcltech.rmg.config.loader;

import com.hcltech.rmg.common.errorsor.ErrorsOr;
import com.hcltech.rmg.config.config.RootConfig;

@FunctionalInterface
public interface IRootConfigBuilder {
    ErrorsOr<RootConfig> create(String rootConfigPath);

    static IRootConfigBuilder fromValue(RootConfig rootConfig) {
        return (ignore) -> ErrorsOr.lift(rootConfig);
    }
}
