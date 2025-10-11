package com.hcltech.rmg.execution.validation.xml;

import com.hcltech.rmg.common.errorsor.ErrorsOr;
import com.hcltech.rmg.config.transformation.XmlTransform;
import com.hcltech.rmg.execution.aspects.AspectExecutor;
import com.hcltech.rmg.xml.XmlTypeClass;
import com.hcltech.rmg.xml.exceptions.XmlValidationException;

import java.util.Map;

public class XmlTransformExecutorForMapStringObject<Schema> implements AspectExecutor<XmlTransform, String, Map<String, Object>> {

    private final XmlTypeClass<Map<String, Object>, Schema> xmlTypeClass;
    private final Map<String, Schema> schemas;

    public XmlTransformExecutorForMapStringObject(XmlTypeClass<Map<String, Object>, Schema> xmlTypeClass, Map<String, Schema> schemas) {
        this.xmlTypeClass = xmlTypeClass;
        this.schemas = schemas;
    }


    @Override
    public Map<String, Object> execute(String key, XmlTransform xmlTransform, String input) {
        Schema schema = schemas.get(xmlTransform.schema());
        if (schema == null) throw new XmlValidationException("No schema found for name " + xmlTransform.schema());
        return xmlTypeClass.parseAndValidate(input, schema);
    }
}
