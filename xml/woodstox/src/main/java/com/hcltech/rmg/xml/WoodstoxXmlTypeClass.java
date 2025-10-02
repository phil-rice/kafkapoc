package com.hcltech.rmg.xml;

import com.hcltech.rmg.common.errorsor.ErrorsOr;
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
import javax.xml.stream.XMLStreamException;
import java.io.InputStream;
import java.io.StringReader;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;

/**
 * Woodstox/StAX2 implementation that compiles XSD to XMLValidationSchema
 * and validates as you pull events (single pass, no DOM).
 *
 * Key extraction is done with a simple slash path passed into extractId(),
 * e.g. "Envelope/Body/Order/Id". Matching is by local name only.
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

    // --- XmlTypeClass ---

    @Override
    public XMLValidationSchema loadSchema(String schemaName, InputStream schemaStream) {
        try (schemaStream) {
            XMLValidationSchemaFactory f =
                    XMLValidationSchemaFactory.newInstance(XMLValidationSchema.SCHEMA_ID_W3C_SCHEMA);
            return f.createSchema(schemaStream);
        } catch (Exception e) {
            throw new LoadSchemaException("Failed to compile XSD for " + schemaName, e);
        }
    }

    @Override
    public ErrorsOr<Map<String, Object>> parseAndValidate(String xml, XMLValidationSchema schema) {
        XMLStreamReader2 r = null;
        try {
            r = (XMLStreamReader2) factory.createXMLStreamReader(new StringReader(xml));

            // Attach schema — validation occurs as we pull
            Validatable v = (Validatable) r;
            v.validateAgainst(schema);
            v.setValidationProblemHandler(new ThrowingProblems());

            CelFriendlyStreamingMapBuilder b = new CelFriendlyStreamingMapBuilder();
            for (;;) {
                switch (r.getEventType()) {
                    case XMLStreamConstants.START_ELEMENT -> b.onStart(r);
                    case XMLStreamConstants.CHARACTERS, XMLStreamConstants.CDATA, XMLStreamConstants.SPACE -> b.onText(r);
                    case XMLStreamConstants.END_ELEMENT -> b.onEnd(r);
                    default -> { /* ignore COMMENT, DTD, PI */ }
                }
                if (!r.hasNext()) break;
                r.next();
            }
            return ErrorsOr.lift(Collections.unmodifiableMap(b.result()));
        } catch (Exception e) {
            return ErrorsOr.error("Parsing xml {0}: {1}", e);
        } finally {
            if (r != null) try { r.close(); } catch (Exception ignore) {}
        }
    }

    // --- XmlKeyExtractor ---

    @Override
    public ErrorsOr<String> extractId(String xml, String idPath) {
        if (xml == null) return ErrorsOr.error("XML was null");
        String[] path = Arrays.stream(idPath.split("/"))
                .map(String::trim)
                .filter(s -> !s.isEmpty())
                .toArray(String[]::new);
        if (path.length == 0) return ErrorsOr.error("idPath cannot be empty");

        XMLStreamReader2 r = null;
        try {
            r = (XMLStreamReader2) factory.createXMLStreamReader(new StringReader(xml));
            int matchIdx = 0;

            for (;;) {
                final int ev = r.getEventType();
                if (ev == XMLStreamConstants.START_ELEMENT) {
                    final String local = r.getLocalName();
                    if (localEquals(local, path[matchIdx])) {
                        if (matchIdx == path.length - 1) {
                            String id = readElementText(r);
                            if (id == null || id.isEmpty()) {
                                return ErrorsOr.error("Key element <%s> was empty", new RuntimeException("empty key"));
                            }
                            return ErrorsOr.lift(id);
                        } else {
                            matchIdx++;
                        }
                    }
                } else if (ev == XMLStreamConstants.END_ELEMENT) {
                    if (matchIdx > 0 && localEquals(r.getLocalName(), path[matchIdx - 1])) {
                        matchIdx--;
                    }
                }
                if (!r.hasNext()) break;
                r.next();
            }
            return ErrorsOr.error("Key not found at path: " + idPath);
        } catch (XMLStreamException e) {
            return ErrorsOr.error("XML key extraction failed: {0}: {1}", e);
        } finally {
            if (r != null) try { r.close(); } catch (Exception ignore) {}
        }
    }

    // --- helpers ---

    private static boolean localEquals(String a, String b) {
        return a != null && a.equals(b);
    }

    private static String readElementText(XMLStreamReader2 r) throws XMLStreamException {
        StringBuilder sb = new StringBuilder(64);
        int depth = 1;
        while (r.hasNext()) {
            int ev = r.next();
            switch (ev) {
                case XMLStreamConstants.START_ELEMENT -> depth++;
                case XMLStreamConstants.CHARACTERS, XMLStreamConstants.CDATA -> sb.append(r.getText());
                case XMLStreamConstants.END_ELEMENT -> {
                    depth--;
                    if (depth == 0) {
                        return sb.toString().trim();
                    }
                }
                default -> { /* ignore others */ }
            }
        }
        return sb.toString().trim();
    }

    private static final class ThrowingProblems implements ValidationProblemHandler {
        @Override
        public void reportProblem(XMLValidationProblem p) {
            int line = p.getLocation() != null ? p.getLocation().getLineNumber() : -1;
            int col = p.getLocation() != null ? p.getLocation().getColumnNumber() : -1;
            String msg = "[" + p.getSeverity() + "] line " + line + ", col " + col + ": " + p.getMessage();
            throw new XmlValidationException(msg);
        }
    }
}
