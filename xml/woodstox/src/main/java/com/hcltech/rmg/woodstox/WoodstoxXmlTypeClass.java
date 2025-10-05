package com.hcltech.rmg.woodstox;

import com.hcltech.rmg.common.errorsor.ErrorsOr;
import com.hcltech.rmg.xml.XmlTypeClass;
import com.hcltech.rmg.xml.exceptions.LoadSchemaException;
import com.hcltech.rmg.xml.exceptions.XmlValidationException;
import org.codehaus.stax2.XMLInputFactory2;
import org.codehaus.stax2.XMLStreamReader2;
import org.codehaus.stax2.validation.*;

import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamException;
import java.io.InputStream;
import java.io.StringReader;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Woodstox/StAX2 implementation that:
 * • compiles XSDs into {@link XMLValidationSchema}s
 * • parses/validates XML streams in a single pass
 * • extracts keys using a streaming pull parser (no DOM)
 * <p>
 * Thread-safe, using one {@link XMLInputFactory2} per thread.
 */
public final class WoodstoxXmlTypeClass implements XmlTypeClass<XMLValidationSchema> {

    private static final ThreadLocal<XMLInputFactory2> FACTORY = ThreadLocal.withInitial(() -> {
        XMLInputFactory2 f = (XMLInputFactory2) XMLInputFactory.newInstance();
        f.setProperty(XMLInputFactory.SUPPORT_DTD, Boolean.FALSE);
        f.setProperty(XMLInputFactory.IS_SUPPORTING_EXTERNAL_ENTITIES, Boolean.FALSE);
        f.setProperty(XMLInputFactory.IS_COALESCING, Boolean.FALSE);
        f.setXMLResolver((publicId, systemId, baseURI, ns) -> null);
        return f;
    });

    // ---------- TypeClass ----------

    @Override
    public XMLValidationSchema loadSchema(String schemaName, InputStream schemaStream) {
        try (schemaStream) {
            XMLValidationSchemaFactory factory =
                    XMLValidationSchemaFactory.newInstance(XMLValidationSchema.SCHEMA_ID_W3C_SCHEMA);
            return factory.createSchema(schemaStream);
        } catch (Exception e) {
            throw new LoadSchemaException("Failed to compile XSD for " + schemaName, e);
        }
    }

    @Override
    public ErrorsOr<Map<String, Object>> parseAndValidate(String xml, XMLValidationSchema schema) {
        XMLStreamReader2 r = null;
        try {
            r = (XMLStreamReader2) FACTORY.get().createXMLStreamReader(new StringReader(xml));

            // attach schema and problem handler
            Validatable v = (Validatable) r;
            v.validateAgainst(schema);
            v.setValidationProblemHandler(new ThrowingProblems());

            CelFriendlyStreamingMapBuilder b = new CelFriendlyStreamingMapBuilder();
            while (r.hasNext()) {
                switch (r.getEventType()) {
                    case XMLStreamConstants.START_ELEMENT -> b.onStart(r);
                    case XMLStreamConstants.CHARACTERS, XMLStreamConstants.CDATA, XMLStreamConstants.SPACE ->
                            b.onText(r);
                    case XMLStreamConstants.END_ELEMENT -> b.onEnd(r);
                    default -> { /* ignore */ }
                }
                r.next();
            }
            return ErrorsOr.lift(Collections.unmodifiableMap(b.result()));
        } catch (Exception e) {
            return ErrorsOr.error("Parsing xml " + e);
        } finally {
            if (r != null) try {
                r.close();
            } catch (Exception ignore) {
            }
        }
    }

    // ---------- KeyExtractor ----------

    @Override
    public ErrorsOr<String> extractId(String rawString, List<String> idPath) {
        if (rawString == null) return ErrorsOr.error("XML was null");
        if (idPath == null || idPath.isEmpty()) return ErrorsOr.error("idPath cannot be empty");

        // pre-trim & skip blanks
        final String[] path = idPath.stream()
                .filter(s -> s != null && !s.isBlank())
                .map(String::trim)
                .toArray(String[]::new);
        if (path.length == 0) return ErrorsOr.error("idPath cannot be empty");

        final String pathStr = String.join("/", path);

        XMLStreamReader2 r = null;
        try {
            r = (XMLStreamReader2) FACTORY.get().createXMLStreamReader(new StringReader(rawString));
            int matchIdx = 0;

            while (true) {
                final int ev = r.getEventType();

                if (ev == XMLStreamConstants.START_ELEMENT) {
                    if (matchIdx < path.length) {
                        final String local = r.getLocalName();
                        if (localEquals(local, path[matchIdx])) {
                            if (matchIdx == path.length - 1) {
                                String id = readElementText(r);
                                if (id == null || id.isEmpty()) {
                                    return ErrorsOr.error("Key element <" + local + "> at path '" + pathStr + "' was empty");
                                }
                                return ErrorsOr.lift(id);
                            } else {
                                matchIdx++;
                            }
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
            return ErrorsOr.error("Key not found at path '" + pathStr + "'");
        } catch (XMLStreamException e) {
            return ErrorsOr.error("XML key extraction failed at path '" + pathStr + "': " + e);
        } finally {
            if (r != null) try {
                r.close();
            } catch (Exception ignore) {
            }
        }
    }

    // ---------- helpers ----------

    private static boolean localEquals(String a, String b) {
        return a != null && b != null && a.equals(b);
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
                    if (depth == 0) return sb.toString().trim();
                }
                default -> { /* ignore */ }
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
