package com.hcltech.rmg.config.configs;

import com.fasterxml.jackson.annotation.JsonValue;
import com.hcltech.rmg.common.errorsor.ErrorsOr;
import com.hcltech.rmg.config.config.BehaviorConfig;
import com.hcltech.rmg.config.config.Config;

import java.io.Serializable;
import java.util.Map;


public record Configs(@JsonValue Map<String, Config> keyToConfigMap) implements Serializable {
    public static String composeKey(String paramKey, String eventName, String moduleName) {
        // If you already have a canonical key util that accepts paramKey, call it here instead.
        // e.g., return KeyUtil.configKey(paramKey, BehaviorConfig.bizlogicAspectName, eventName, moduleName);
        String behaviorKey = BehaviorConfig.configKey(moduleName, BehaviorConfig.bizlogicAspectName, eventName);
        return paramKey + "::" + behaviorKey;
    }

    public ErrorsOr<Config> getConfig(String key) {
        Config config = keyToConfigMap.get(key);
        return (config == null)
                ? ErrorsOr.error("Cannot find config for key: " + key + ", available keys: " + keyToConfigMap.keySet())
                : ErrorsOr.lift(config);
    }
}
