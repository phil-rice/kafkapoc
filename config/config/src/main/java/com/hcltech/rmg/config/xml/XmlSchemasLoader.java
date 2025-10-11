// src/main/java/com/hcltech/rmg/config/xml/XmlSchemasLoader.java
package com.hcltech.rmg.config.xml;

import com.hcltech.rmg.common.resources.ClasspathLSResourceResolver;
import com.hcltech.rmg.common.resources.LoadFromInputStream;
import com.hcltech.rmg.common.resources.ResourceCompiler;
import com.hcltech.rmg.common.resources.ResourceStoreBuilder;
import com.hcltech.rmg.config.config.BehaviorConfig;
import com.hcltech.rmg.config.config.BehaviorConfigVisitor;
import com.hcltech.rmg.config.config.BehaviorConfigWalker;
import com.hcltech.rmg.config.transformation.XsltTransform;

import javax.xml.XMLConstants;
import javax.xml.transform.stream.StreamSource;
import javax.xml.validation.Schema;
import javax.xml.validation.SchemaFactory;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

/**
 * Collects and compiles XSDs referenced from {@link BehaviorConfig}, resolving includes/imports
 * from the classpath (no network/disk) under a given resource prefix.
 */
public interface XmlSchemasLoader {

    /**
     * 1) Collect logical schema names (deduped, order-preserving).
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
     * 2) Compile a set of schema resource names under a given prefix into a Map&lt;String, Schema&gt;.
     * Security: FEATURE_SECURE_PROCESSING enabled; external DTD/Schema access disabled.
     * Includes/imports are resolved from the classpath via a "classpath:/" systemId
     * and {@link ClasspathLSResourceResolver}.
     *
     * @param resourcePrefix e.g. "schemas/"
     * @param schemaNames    logical names, e.g. ["a.xsd","nested/b.xsd"]
     * @param parallelism    0 = auto; otherwise fixed thread count
     */
    static Map<String, Schema> compileSchemas(String resourcePrefix,
                                              Collection<String> schemaNames,
                                              int parallelism) {
        if (schemaNames == null || schemaNames.isEmpty()) return Map.of();

        ResourceCompiler<Schema> schemaCompiler = (resourceName) ->
                LoadFromInputStream.loadFromClasspath(resourcePrefix, resourceName, in -> {
                    SchemaFactory f = SchemaFactory.newInstance(XMLConstants.W3C_XML_SCHEMA_NS_URI);

                    // Harden: deterministic, no external fetches
                    f.setFeature(XMLConstants.FEATURE_SECURE_PROCESSING, true);
                    f.setProperty(XMLConstants.ACCESS_EXTERNAL_DTD, "");
                    f.setProperty(XMLConstants.ACCESS_EXTERNAL_SCHEMA, "");

                    // Resolve includes/imports from classpath only
                    f.setResourceResolver(new ClasspathLSResourceResolver(
                            Thread.currentThread().getContextClassLoader()
                    ));

                    // Use a classpath systemId so the resolver is invoked with a classpath base URI
                    String systemId = "classpath:/" + resourcePrefix + resourceName;

                    return f.newSchema(new StreamSource(in, systemId));
                });

        return ResourceStoreBuilder
                .with(schemaCompiler)
                .names(schemaNames)
                .parallelism(parallelism)
                .build();
    }

    /**
     * 3) Stitch: collect then compile (auto parallelism).
     */
    static Map<String, Schema> loadSchemas(BehaviorConfig config, String resourcePrefix) {
        return compileSchemas(resourcePrefix, collectSchemaNames(config), 0);
    }

    /**
     * Overload to control parallelism.
     */
    static Map<String, Schema> loadSchemas(BehaviorConfig config, String resourcePrefix, int parallelism) {
        return compileSchemas(resourcePrefix, collectSchemaNames(config), parallelism);
    }
}
