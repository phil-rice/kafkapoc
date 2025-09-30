package com.hcltech.rmg.xml;

import com.hcltech.rmg.xml.exceptions.LoadSchemaException;
import com.hcltech.rmg.xml.exceptions.XmlValidationException;
import org.codehaus.stax2.XMLInputFactory2;
import org.codehaus.stax2.XMLStreamReader2;
import org.codehaus.stax2.validation.Validatable;
import org.codehaus.stax2.validation.XMLValidationProblem;
import org.codehaus.stax2.validation.ValidationProblemHandler;
import org.codehaus.stax2.validation.XMLValidationSchema;
import org.codehaus.stax2.validation.XMLValidationSchemaFactory;

import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamConstants;
import java.io.InputStream;
import java.io.StringReader;
import java.util.Collections;
import java.util.Map;

/**
 * Woodstox/StAX2 implementation that compiles XSD to XMLValidationSchema
 * and validates as you pull events (single pass, no DOM).
 *
 * Output map shape is produced by CelFriendlyStreamingMapBuilder:
 *   - element keys: CEL-safe identifiers
 *   - attributes under "attr" sub-map
 *   - text under "text"
 *   - repeats: always lists
 *   - order: LinkedHashMap (stable)
 */
public final class WoodstoxXmlTypeClass implements XmlTypeClass<XMLValidationSchema> {

    private final XMLInputFactory2 factory;

    public WoodstoxXmlTypeClass() {
        this.factory = (XMLInputFactory2) XMLInputFactory.newInstance();
        // Security & perf posture
        factory.setProperty(XMLInputFactory.SUPPORT_DTD, Boolean.FALSE);
        factory.setProperty(XMLInputFactory.IS_SUPPORTING_EXTERNAL_ENTITIES, Boolean.FALSE);
        factory.setProperty(XMLInputFactory.IS_COALESCING, Boolean.FALSE); // faster; builder coalesces text
        factory.setXMLResolver((publicId, systemId, baseURI, ns) -> null); // no external fetches
    }

    @Override
    public XMLValidationSchema loadSchema(String schemaName, InputStream schemaStream) {
        try (schemaStream) {
            XMLValidationSchemaFactory f =
                    XMLValidationSchemaFactory.newInstance(XMLValidationSchema.SCHEMA_ID_W3C_SCHEMA);
            // Woodstox 7.x: create from InputStream (single-file XSD path)
            return f.createSchema(schemaStream);
        } catch (Exception e) {
            throw new LoadSchemaException("Failed to compile XSD for " + schemaName, e);
        }
    }

    @Override
    public Map<String, Object> parseAndValidate(String xml, XMLValidationSchema schema) {
        XMLStreamReader2 r = null;
        try {
            r = (XMLStreamReader2) factory.createXMLStreamReader(new StringReader(xml));

            // Attach schema — validation occurs as we pull
            Validatable v = (Validatable) r;
            v.validateAgainst(schema);
            v.setValidationProblemHandler(new ThrowingProblems());

            // Stream to CEL-friendly map
            CelFriendlyStreamingMapBuilder b = new CelFriendlyStreamingMapBuilder();
            for (;;) {
                switch (r.getEventType()) {
                    case XMLStreamConstants.START_ELEMENT -> b.onStart(r);
                    case XMLStreamConstants.CHARACTERS, XMLStreamConstants.CDATA, XMLStreamConstants.SPACE -> b.onText(r);
                    case XMLStreamConstants.END_ELEMENT -> b.onEnd(r);
                    default -> { /* ignore: COMMENT, DTD, PI */ }
                }
                if (!r.hasNext()) break;
                r.next();
            }
            return Collections.unmodifiableMap(b.result());
        } catch (RuntimeException e) {
            throw e; // already our unchecked types or user code
        } catch (Exception e) {
            throw new XmlValidationException("XML parse/validate failure", e);
        } finally {
            if (r != null) try { r.close(); } catch (Exception ignore) {}
        }
    }

    /** Fail-fast with location-aware message from Woodstox validation. */
    private static final class ThrowingProblems implements ValidationProblemHandler {
        @Override
        public void reportProblem(XMLValidationProblem p) {
            int line = p.getLocation() != null ? p.getLocation().getLineNumber() : -1;
            int col  = p.getLocation() != null ? p.getLocation().getColumnNumber() : -1;
            String msg = "[" + p.getSeverity() + "] line " + line + ", col " + col + ": " + p.getMessage();
            throw new XmlValidationException(msg);
        }
    }
}
