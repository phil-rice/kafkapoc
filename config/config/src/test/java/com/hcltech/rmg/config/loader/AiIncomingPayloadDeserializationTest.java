package com.hcltech.rmg.config.loader;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.hcltech.rmg.config.ai.AiIncomingPayload;
import com.hcltech.rmg.config.bizlogic.CelInlineLogic;
import com.hcltech.rmg.parameters.ParameterConfig;
import org.junit.jupiter.api.Test;

import java.io.InputStream;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class AiIncomingPayloadDeserializationTest {

    @Test
    void shouldDeserializeAiPayloadShapeIncludingRootConfig() throws Exception {
        ObjectMapper mapper = new ObjectMapper();

        try (InputStream in = getClass().getClassLoader()
                .getResourceAsStream("ConfigLoaderForAiTest/sample.json")) {

            assertNotNull(in, "sample.json not found on classpath under ConfigLoaderForAiTest");

            AiIncomingPayload payload = mapper.readValue(in, AiIncomingPayload.class);
            assertNotNull(payload);

            // top-level cel
            assertEquals("the projection", payload.celProjection(), "top-level cel must match");

            // --- rootConfig checks ---
            assertNotNull(payload.rootConfig(), "rootConfig must be present");
            assertEquals("/xsd/schema.xsd", payload.rootConfig().xmlSchemaPath(), "xmlSchemaPath mismatch");

            ParameterConfig pc = payload.rootConfig().parameterConfig();
            assertNotNull(pc, "parameterConfig must be present");
            assertNotNull(pc.parameters(), "parameterConfig.parameters must be present");
            assertFalse(pc.parameters().isEmpty(), "parameterConfig.parameters should not be empty");
            assertEquals("A", pc.parameters().get(0).defaultValue());
            assertEquals(3, pc.parameters().get(0).legalValue().size(),
                    "expected 3 legal values in first parameter");

            // --- config param keys ---
            var byParam = payload.config().byParameterKey();
            assertFalse(byParam.isEmpty(), "expected at least one parameter key");
            assertTrue(byParam.containsKey("param1a:param2a"), "param1a:param2a missing");
            assertTrue(byParam.containsKey("param1a:param2b"), "param1a:param2b missing");
            assertTrue(byParam.containsKey("param1b:param2a"), "param1b:param2a missing");

            // walk a param → event → aspect → module
            var events = byParam.get("param1a:param2a").events().byEvent();
            assertTrue(events.containsKey("test-event"), "test-event missing");

            var eventDef = events.get("test-event");
            Map<String, Map<String, CelInlineLogic>> byAspect = eventDef.byAspect();

            assertTrue(byAspect.containsKey("bizlogic"), "bizlogic aspect missing");
            var modules = byAspect.get("bizlogic");

            assertTrue(modules.containsKey("notification"), "notification module missing");
            CelInlineLogic logic = modules.get("notification");
            assertEquals("cel", logic.type(), "logic.type should be 'cel'");
            assertEquals("{'out': [message]}", logic.cel(), "cel body must match sample");
        }
    }
}
