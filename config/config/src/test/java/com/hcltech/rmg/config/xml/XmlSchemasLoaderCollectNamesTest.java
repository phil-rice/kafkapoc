package com.hcltech.rmg.config.xml;

import com.hcltech.rmg.config.aspect.AspectMap;
import com.hcltech.rmg.config.config.BehaviorConfig;
import com.hcltech.rmg.config.fixture.ConfigTestFixture;
import com.hcltech.rmg.config.transformation.XsltTransform;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashSet;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;

class XmlSchemasLoaderCollectNamesTest {

    @Test
    void returns_empty_when_no_xslt_transforms() {
        // given: a config with no transformation aspects
        BehaviorConfig config = ConfigTestFixture.configWith("evt", new AspectMap(ConfigTestFixture.v(), ConfigTestFixture.t(), ConfigTestFixture.e(), ConfigTestFixture.b()));

        // when
        Set<String> names = XmlSchemasLoader.collectSchemaNames(config);

        // then
        assertEquals(Set.of(), names);
    }

    @Test
    void collects_from_single_event_multiple_transforms() {
        // given: one event with two XSLT transforms, including a schema with surrounding spaces
        var am = new AspectMap(
                ConfigTestFixture.v(),
                ConfigTestFixture.t(
                        ConfigTestFixture.kv("xsltA", new XsltTransform("a.xslt", "a.xsd")),
                        ConfigTestFixture.kv("xsltB", new XsltTransform("b.xslt", "  b.xsd  ")) // should be trimmed
                ),
                ConfigTestFixture.e(), ConfigTestFixture.b()
        );
        BehaviorConfig config = ConfigTestFixture.configWith("evt", am);

        // when
        Set<String> names = XmlSchemasLoader.collectSchemaNames(config);

        // then: preserves insertion order
        Set<String> expected = new LinkedHashSet<>();
        expected.add("a.xsd");
        expected.add("b.xsd");
        assertEquals(expected, names);
    }


    @Test
    void de_dupes_across_multiple_events_and_preserves_first_seen_order() {
        // given: two events; second repeats "two.xsd"
        var evt1 = new AspectMap(
                ConfigTestFixture.v(),
                ConfigTestFixture.t(
                        ConfigTestFixture.kv("t1", new XsltTransform("one.xslt", "one.xsd")),
                        ConfigTestFixture.kv("t2", new XsltTransform("two.xslt", "two.xsd"))
                ),
                ConfigTestFixture.e(), ConfigTestFixture.b()
        );
        var evt2 = new AspectMap(
                ConfigTestFixture.v(),
                ConfigTestFixture.t(
                        ConfigTestFixture.kv("t3", new XsltTransform("two-again.xslt", "two.xsd")),
                        ConfigTestFixture.kv("t4", new XsltTransform("three.xslt", "three.xsd"))
                ),
                ConfigTestFixture.e(), ConfigTestFixture.b()
        );

        BehaviorConfig config = new BehaviorConfig(java.util.Map.of(
                "alpha", evt1,
                "beta", evt2
        ));

        // when
        Set<String> names = XmlSchemasLoader.collectSchemaNames(config);

        // then: first-seen order: one.xsd, two.xsd, three.xsd
        Set<String> expected = new LinkedHashSet<>();
        expected.add("one.xsd");
        expected.add("two.xsd");
        expected.add("three.xsd");
        assertEquals(expected, names);
    }

}


