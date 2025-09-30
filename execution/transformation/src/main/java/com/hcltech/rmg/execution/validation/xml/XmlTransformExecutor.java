package com.hcltech.rmg.execution.validation.xml;

import com.hcltech.rmg.common.errorsor.ErrorsOr;
import com.hcltech.rmg.config.transformation.XmlTransform;
import com.hcltech.rmg.execution.aspects.RegisteredAspectExecutor;
import com.hcltech.rmg.xml.XmlTypeClass;

import java.util.List;
import java.util.Map;

public class XmlTransformExecutor<Schema> implements RegisteredAspectExecutor<XmlTransform, String, Map<String, Object>> {

    private final XmlTypeClass<Schema> xmlTypeClass;
    private final Map<String, Schema> schemas;

    public XmlTransformExecutor(XmlTypeClass<Schema> xmlTypeClass, Map<String, Schema> schemas) {
        this.xmlTypeClass = xmlTypeClass;
        this.schemas = schemas;
    }

    @Override
    public ErrorsOr<Map<String, Object>> execute(String key,List<String> modules, String aspect, XmlTransform xmlTransform, String input) {
        Schema schema = schemas.get(xmlTransform.schema());
        if (schema == null) return ErrorsOr.error("No schema found for name " + xmlTransform.schema());
        return xmlTypeClass.parseAndValidate(input, schema);
    }
}
