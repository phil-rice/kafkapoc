package com.hcltech.rmg.config.configs;

import com.hcltech.rmg.common.errorsor.ErrorsOr;
import com.hcltech.rmg.config.config.BehaviorConfig;
import com.hcltech.rmg.config.config.Config;
import com.hcltech.rmg.parameters.OneParameterConfig;
import com.hcltech.rmg.parameters.ParameterConfig;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class ConfigsTest {

    @Test
    void composeKey_buildsExpectedKey_usingBizlogicAspect() {
        String paramKey = "myParam";
        String eventName = "OrderCreated";
        String moduleName = "order";
        String expected = paramKey + "::" +
                BehaviorConfig.configKey(moduleName, BehaviorConfig.bizlogicAspectName, eventName);

        String actual = Configs.composeKey(paramKey, eventName, moduleName);

        assertEquals(expected, actual);
    }

    @Test
    void getConfig_returnsLiftedValue_whenKeyPresent() {
        // Arrange: minimal valid Config
        BehaviorConfig behaviorConfig = BehaviorConfig.empty();
        ParameterConfig parameterConfig = new ParameterConfig(List.of(
                new OneParameterConfig(List.of("A", "B"), "A", "demo param")
        ));
        Config cfg = new Config(behaviorConfig, parameterConfig, "/xsd/demo.xsd");

        Map<String, Config> map = new HashMap<>();
        map.put("present-key", cfg);

        Configs configs = new Configs(map);

        // Act
        ErrorsOr<Config> result = configs.getConfig("present-key");

        // Assert (only rely on valueOrThrow to avoid depending on ErrorsOr internals)
        assertSame(cfg, result.valueOrThrow());
    }

    @Test
    void getConfig_returnsError_whenKeyMissing() {
        // Arrange
        BehaviorConfig behaviorConfig = BehaviorConfig.empty();
        ParameterConfig parameterConfig = new ParameterConfig(List.of());
        Config cfg = new Config(behaviorConfig, parameterConfig, "/xsd/demo.xsd");
        Configs configs = new Configs(Map.of("existing", cfg));

        // Act
        ErrorsOr<Config> missing = configs.getConfig("missing");

        // Assert: calling valueOrThrow() should throw for error cases.
        try {
            missing.valueOrThrow();
            fail("Expected valueOrThrow() to throw for missing key");
        } catch (Exception expected) {
            // pass: we don't assume a specific exception type
        }
    }
}
