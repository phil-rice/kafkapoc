// src/test/java/com/hcltech/rmg/config/config/BehaviorConfigTest.java
package com.hcltech.rmg.config.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.hcltech.rmg.config.aspect.AspectMap;
import com.hcltech.rmg.config.bizlogic.BizLogicAspect;
import com.hcltech.rmg.config.enrich.EnrichmentAspect;
import com.hcltech.rmg.config.transformation.TransformationAspect;
import com.hcltech.rmg.config.validation.ValidationAspect;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;

class BehaviorConfigTest {

    @Test
    void configKey_formats_as_module_colon_aspect_colon_event() {
        assertEquals("mod:validation:evt", BehaviorConfig.configKey("mod", "validation", "evt"));
        assertEquals("m:t:e", BehaviorConfig.configKey("m", "t", "e"));
    }

    @Test
    void empty_returns_config_with_no_events() {
        BehaviorConfig cfg = BehaviorConfig.empty();
        assertNotNull(cfg.events());
        assertTrue(cfg.events().isEmpty());
        // equals/hashCode sanity
        assertEquals(new BehaviorConfig(Map.of()), cfg);
    }

    @Test
    void ctor_with_null_events_coerces_to_empty_map() {
        BehaviorConfig cfg = new BehaviorConfig(null);
        assertNotNull(cfg.events());
        assertTrue(cfg.events().isEmpty());
    }

    @Test
    void allModuleNames_unions_and_sorts_keys_across_aspects() {
        // We only care about keys; values can be null.
        var val1 = new java.util.HashMap<String, com.hcltech.rmg.config.validation.ValidationAspect>();
        val1.put("valA", null);

        var tr1 = new java.util.HashMap<String, com.hcltech.rmg.config.transformation.TransformationAspect>();
        tr1.put("trB", null);

        var enr1 = new java.util.HashMap<String, com.hcltech.rmg.config.enrich.EnrichmentAspect>();
        enr1.put("enrX", null); // ignored by allModuleNames

        var biz1 = new java.util.HashMap<String, com.hcltech.rmg.config.bizlogic.BizLogicAspect>();
        biz1.put("bizZ", null);

        AspectMap evt1 = new AspectMap(val1, tr1, enr1, biz1);

        // Second event: duplicate valA, plus trC and bizM
        var val2 = new java.util.HashMap<String, com.hcltech.rmg.config.validation.ValidationAspect>();
        val2.put("valA", null);

        var tr2 = new java.util.HashMap<String, com.hcltech.rmg.config.transformation.TransformationAspect>();
        tr2.put("trC", null);

        var enr2 = java.util.Map.<String, com.hcltech.rmg.config.enrich.EnrichmentAspect>of(); // empty

        var biz2 = new java.util.HashMap<String, com.hcltech.rmg.config.bizlogic.BizLogicAspect>();
        biz2.put("bizM", null);

        AspectMap evt2 = new AspectMap(val2, tr2, enr2, biz2);

        BehaviorConfig cfg = new BehaviorConfig(java.util.Map.of(
                "event1", evt1,
                "event2", evt2
        ));

        var modules = cfg.allModuleNames();

        // Should be alphabetical: bizM, bizZ, trB, trC, valA
        assertEquals(java.util.List.of("bizM", "bizZ", "trB", "trC", "valA"), modules);
    }


    @Test
    void jackson_deserializes_and_ignores_unknown_fields() throws Exception {
        ObjectMapper om = new ObjectMapper();
        String json = """
            {
              "events": {
                "evt": {
                  "validation": {},
                  "transformation": {},
                  "enrichment": {},
                  "bizlogic": {}
                }
              },
              "extraIgnoredField": 123
            }
            """;

        BehaviorConfig cfg = om.readValue(json, BehaviorConfig.class);
        assertTrue(cfg.events().containsKey("evt"));
        assertEquals(1, cfg.events().size());
    }

    @Test
    void jackson_requires_events_field_but_allows_empty_object() throws Exception {
        ObjectMapper om = new ObjectMapper();
        BehaviorConfig cfg = om.readValue("{\"events\":{}}", BehaviorConfig.class);
        assertNotNull(cfg.events());
        assertTrue(cfg.events().isEmpty());
    }
}
