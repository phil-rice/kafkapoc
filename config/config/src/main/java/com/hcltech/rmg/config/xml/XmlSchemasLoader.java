package com.hcltech.rmg.config.xml;

import com.hcltech.rmg.config.config.BehaviorConfig;
import com.hcltech.rmg.config.config.BehaviorConfigVisitor;
import com.hcltech.rmg.config.config.BehaviorConfigWalker;
import com.hcltech.rmg.config.transformation.XsltTransform;
import com.hcltech.rmg.common.resources.LoadFromInputStream;
import com.hcltech.rmg.common.resources.ResourceCompiler;
import com.hcltech.rmg.common.resources.ResourceStoreBuilder;

import javax.xml.XMLConstants;
import javax.xml.transform.stream.StreamSource;
import javax.xml.validation.Schema;
import javax.xml.validation.SchemaFactory;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

public interface XmlSchemasLoader {

    /**
     * 1) Collect all schema names referenced in the config (deduped, order-preserving).
     */
    static Set<String> collectSchemaNames(BehaviorConfig config) {
        final Set<String> names = new LinkedHashSet<>();
        BehaviorConfigWalker.walk(config, new BehaviorConfigVisitor() {
            @Override
            public void onXsltTransform(String eventName, String name, XsltTransform t) {
                final String s = t.schema();
                if (s == null || s.trim().isEmpty()) {
                    throw new IllegalArgumentException(
                            "Missing schema for XSLT transform '" + name + "' in event '" + eventName + "'"
                    );
                }
                names.add(s.trim());
            }

        });
        return names;
    }

    /**
     * 2) Compile a set of schema resource names under a given prefix into a Map<String, Schema>.
     *    Uses secure settings and resolves relative includes via systemId (prefix + name).
     *
     * @param resourcePrefix e.g. "schemas/"
     * @param schemaNames    logical names collected from config, e.g. ["a.xsd","nested/b.xsd"]
     * @param parallelism    0 = auto (min(names, cores)); otherwise fixed thread count
     */
    static Map<String, Schema> compileSchemas(String resourcePrefix,
                                              Collection<String> schemaNames,
                                              int parallelism) {
        if (schemaNames == null || schemaNames.isEmpty()) return Map.of();

        ResourceCompiler<Schema> schemaCompiler = (resourceName) ->
                LoadFromInputStream.loadFromClasspath(resourcePrefix, resourceName, in -> {
                    SchemaFactory f = SchemaFactory.newInstance(XMLConstants.W3C_XML_SCHEMA_NS_URI);
                    // Hardening + deterministic, no network fetches:
                    f.setFeature(XMLConstants.FEATURE_SECURE_PROCESSING, true);
                    f.setProperty(XMLConstants.ACCESS_EXTERNAL_DTD, "");
                    f.setProperty(XMLConstants.ACCESS_EXTERNAL_SCHEMA, "");
                    // systemId ensures xs:include/import with relative paths resolve under resourcePrefix
                    String systemId = resourcePrefix + resourceName;
                    return f.newSchema(new StreamSource(in, systemId));
                });

        return ResourceStoreBuilder
                .with(schemaCompiler)
                .names(schemaNames)
                .parallelism(parallelism)
                .build();
    }

    /**
     * 3) Stitch: collect from config, then compile with the given prefix (auto parallelism).
     */
    static Map<String, Schema> loadSchemas(BehaviorConfig config, String resourcePrefix) {
        return compileSchemas(resourcePrefix, collectSchemaNames(config), 0);
    }

    // Optional convenience overload if you want the caller to control parallelism:
    static Map<String, Schema> loadSchemas(BehaviorConfig config, String resourcePrefix, int parallelism) {
        return compileSchemas(resourcePrefix, collectSchemaNames(config), parallelism);
    }
}
