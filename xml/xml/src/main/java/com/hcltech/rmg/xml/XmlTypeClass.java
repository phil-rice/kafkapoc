package com.hcltech.rmg.xml;

import com.hcltech.rmg.common.errorsor.ErrorsOr;
import com.hcltech.rmg.common.resources.LoadFromInputStream;
import com.hcltech.rmg.common.resources.ResourceStoreBuilder;

import java.io.InputStream;
import java.util.List;
import java.util.Map;

public interface XmlTypeClass<Schema> extends XmlKeyExtractor{
    Schema loadSchema(String schemaName, InputStream schemaStream);

    ErrorsOr<Map<String, Object>> parseAndValidate(String xml, Schema schema);

    static <Schema> Map<String, Schema> loadSchemas(XmlTypeClass<Schema> tc, String resourcePrefix, List<String> schemaNames) {
        return ResourceStoreBuilder.<Schema>with(schemaName ->
                LoadFromInputStream.<Schema>loadFromClasspath(resourcePrefix, schemaName,
                        in -> tc.loadSchema(schemaName, in))
        ).names(schemaNames).build();

    }
}
