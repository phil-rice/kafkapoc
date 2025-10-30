package com.hcltech.rmg.config.loader;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.hcltech.rmg.config.ai.AiIncomingPayload;
import com.hcltech.rmg.config.ai.AiPayloadToConfigs;
import com.hcltech.rmg.config.aspect.AspectMap;
import com.hcltech.rmg.config.bizlogic.CelInlineLogic;

import com.hcltech.rmg.config.config.BehaviorConfig;
import com.hcltech.rmg.config.config.Config;
import com.hcltech.rmg.config.configs.Configs;
import org.junit.jupiter.api.Test;

import java.io.InputStream;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

class AiPayloadToConfigsMappingTest {

    @Test
    void shouldMapAiPayloadToCanonicalConfigs_UsingRootConfigFromPayload() throws Exception {
        ObjectMapper mapper = new ObjectMapper();

        AiIncomingPayload ai;
        try (InputStream in = getClass().getClassLoader()
                .getResourceAsStream("ConfigLoaderForAiTest/sample.json")) {
            assertNotNull(in, "sample.json not found on classpath under ConfigLoaderForAiTest");
            ai = mapper.readValue(in, AiIncomingPayload.class);
        }

        // Note: now we pass payload.rootConfig() to the mapper
        Configs cfgs = AiPayloadToConfigs.toConfigs(ai);
        Map<String, Config> map = cfgs.keyToConfigMap();
        assertFalse(map.isEmpty(), "Configs should not be empty");

        String eventName = "test-event";
        String moduleName = "notification";

        String k1 = "param1a:param2a";
        String k2 = "param1a:param2b";
        String k3 = "param1b:param2a";

        assertTrue(map.containsKey(k1), "Missing key: " + k1);
        assertTrue(map.containsKey(k2), "Missing key: " + k2);
        assertTrue(map.containsKey(k3), "Missing key: " + k3);

        // Inspect one config thoroughly
        Config c = map.get(k1);
        assertNotNull(c);

        // From rootConfig
        assertEquals(ai.rootConfig().parameterConfig(), c.parameterConfig());
        assertEquals(ai.rootConfig().xmlSchemaPath(), c.xmlSchemaPath());

        // celForAi propagated
        assertEquals(ai.celProjection(), c.celForAi());

        // BehaviorConfig wiring
        BehaviorConfig behavior = c.behaviorConfig();
        assertNotNull(behavior);
        assertTrue(behavior.events().containsKey(eventName), "event missing in BehaviorConfig");

        AspectMap aspects = behavior.events().get(eventName);
        assertNotNull(aspects);

        assertTrue(aspects.validation().isEmpty(), "validation should be empty");
        assertTrue(aspects.transformation().isEmpty(), "transformation should be empty");
        assertTrue(aspects.enrichment().isEmpty(), "enrichment should be empty");

        Map<String, ?> biz = aspects.bizlogic();
        assertEquals(1, biz.size(), "expected exactly one bizlogic module");
        assertTrue(biz.containsKey(moduleName), "bizlogic must contain 'notification'");

        Object v = biz.get(moduleName);
        assertInstanceOf(CelInlineLogic.class, v, "bizlogic module should be CelInlineLogic");
        CelInlineLogic cel = (CelInlineLogic) v;
        assertEquals("cel", cel.type());
        assertEquals("{'out': [message]}", cel.cel());
    }

    @Test
    void shouldListAllGeneratedConfigKeys() throws Exception {
        ObjectMapper mapper = new ObjectMapper();

        AiIncomingPayload ai;
        try (InputStream in = getClass().getClassLoader()
                .getResourceAsStream("ConfigLoaderForAiTest/sample.json")) {
            assertNotNull(in, "sample.json not found on classpath under ConfigLoaderForAiTest");
            ai = mapper.readValue(in, AiIncomingPayload.class);
        }

        Configs cfgs = AiPayloadToConfigs.toConfigs(ai);
        Map<String, Config> map = cfgs.keyToConfigMap();
        assertFalse(map.isEmpty(), "No configs generated from payload");

        System.out.println("=== Generated Config Keys (" + map.size() + ") ===");
        map.keySet().forEach(k -> System.out.println("  " + k));

        var expectedKeys = Set.of(
                "param1a:param2a",
                "param1a:param2b",
                "param1b:param2a"
        );
        assertEquals(expectedKeys, map.keySet(), "Generated keys differ from expected");
    }
}
