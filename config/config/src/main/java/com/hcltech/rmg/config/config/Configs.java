package com.hcltech.rmg.config.config;

import com.hcltech.rmg.common.errorsor.ErrorsOr;

import java.util.Map;

public record Configs(Map<String, Config> keyToConfigMap) {
    public ErrorsOr<Config> getConfig(String key) {
        Config config = keyToConfigMap.get(key);
        return (config == null)
                ? ErrorsOr.error("Cannot find config for key: " + key + ", available keys: " + keyToConfigMap.keySet())
                : ErrorsOr.lift(config);
    }
}
