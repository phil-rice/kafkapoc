package com.hcltech.rmg.woodstox;

import com.hcltech.rmg.common.errorsor.ErrorsOr;
import com.hcltech.rmg.xml.XmlTypeClass;
import com.hcltech.rmg.xml.exceptions.XmlValidationException;
import org.codehaus.stax2.XMLInputFactory2;
import org.codehaus.stax2.XMLStreamReader2;

import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamException;
import java.io.InputStream;
import java.io.StringReader;
import java.util.*;

/**
 * Woodstox/StAX2 parser that:
 * • parses XML streams in a single pass (no validation)
 * • extracts keys using a streaming pull parser (no DOM)
 * • builds a CEL-friendly Map shape
 * <p>
 * Thread-safe: one {@link XMLInputFactory2} per thread.
 */
public final class WoodstoxXmlForMapStringObjectTypeClassNoValidation<Schema>
        implements XmlTypeClass<Map<String, Object>, Schema> {

    private static final ThreadLocal<XMLInputFactory2> FACTORY = ThreadLocal.withInitial(() -> {
        XMLInputFactory2 f = (XMLInputFactory2) XMLInputFactory.newInstance();
        // Safety defaults
        f.setProperty(XMLInputFactory.SUPPORT_DTD, Boolean.FALSE);
        f.setProperty(XMLInputFactory.IS_SUPPORTING_EXTERNAL_ENTITIES, Boolean.FALSE);
        f.setProperty(XMLInputFactory.IS_COALESCING, Boolean.FALSE);
        f.setProperty(XMLInputFactory.IS_NAMESPACE_AWARE, Boolean.TRUE);
        f.setXMLResolver((publicId, systemId, baseURI, ns) -> null);
        return f;
    });

    // ---------- TypeClass (schema-free) ----------

    @Override
    public Schema loadSchema(String schemaName, InputStream schemaStream) {
        return (Schema) new Object();
    }

    @Override
    public Map<String, Object> parseAndValidate(String xml, Schema ignored) throws XmlValidationException {
        XMLStreamReader2 r = null;
        try {
            r = (XMLStreamReader2) FACTORY.get().createXMLStreamReader(new StringReader(xml));

            // Single-pass streaming build
            CelFriendlyStreamingMapBuilder b = new CelFriendlyStreamingMapBuilder();
            while (r.hasNext()) {
                int ev = r.getEventType();
                switch (ev) {
                    case XMLStreamConstants.START_ELEMENT -> b.onStart(r);
                    case XMLStreamConstants.CHARACTERS, XMLStreamConstants.CDATA, XMLStreamConstants.SPACE ->
                            b.onText(r);
                    case XMLStreamConstants.END_ELEMENT -> b.onEnd(r);
                    default -> { /* ignore */ }
                }
                r.next();
            }
            return b.result();
        } catch (XMLStreamException e) {
            throw new XmlValidationException("XML parsing failed: " + e.getMessage(), e);
        } catch (Exception e) {
            throw new XmlValidationException("Unexpected parsing error: " + e.getMessage(), e);
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
                                if (id == null || id.trim().isEmpty()) {
                                    return ErrorsOr.error("Key element <" + local + "> at path '" + pathStr + "' was empty");
                                }
                                return ErrorsOr.lift(id.trim());
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

    /**
     * Reads element text for the current START_ELEMENT position and returns
     * the coalesced text when the matching END_ELEMENT is reached.
     */
    private static String readElementText(XMLStreamReader2 r) throws XMLStreamException {
        StringBuilder sb = new StringBuilder(64);
        int depth = 1;
        while (r.hasNext()) {
            int ev = r.next();
            switch (ev) {
                case XMLStreamConstants.START_ELEMENT -> depth++;
                case XMLStreamConstants.CHARACTERS, XMLStreamConstants.CDATA, XMLStreamConstants.SPACE ->
                        sb.append(r.getText());
                case XMLStreamConstants.END_ELEMENT -> {
                    depth--;
                    if (depth == 0) return sb.toString().trim();
                }
                default -> { /* ignore */ }
            }
        }
        return sb.toString().trim();
    }

    // ---------- Streaming map builder (repeats → lists) ----------

    /**
     * Builds a CEL-friendly map in a single pass:
     * - Leaf element (no attrs/children) → plain String (trimmed text)
     * - Non-leaf → Map with:
     * "attr": {...}        (only if attributes exist)
     * childName: childVal  (each child)
     * "text": "..."        (only if mixed content non-blank)
     * Repeated child names are promoted to List preserving document order.
     */
    private static final class CelFriendlyStreamingMapBuilder {
        private static final class Node {
            final String name;
            final StringBuilder text = new StringBuilder();
            final Map<String, Object> attrs = new LinkedHashMap<>();
            final Map<String, Object> children = new LinkedHashMap<>();

            Node(String name) {
                this.name = name;
            }

            boolean hasOnlyText() {
                return attrs.isEmpty() && children.isEmpty();
            }
        }

        private final Deque<Node> stack = new ArrayDeque<>();
        private final Map<String, Object> result = new LinkedHashMap<>();

        Map<String, Object> result() {
            return result;
        }

        void onStart(XMLStreamReader2 r) {
            Node n = new Node(r.getLocalName());
            for (int i = 0; i < r.getAttributeCount(); i++) {
                n.attrs.put(r.getAttributeLocalName(i), r.getAttributeValue(i));
            }
            stack.push(n);
        }

        void onText(XMLStreamReader2 r) {
            if (!stack.isEmpty()) stack.peek().text.append(r.getText());
        }

        void onEnd(XMLStreamReader2 r) {
            Node n = stack.pop();

            Object value;
            if (n.hasOnlyText()) {
                value = n.text.toString().trim();
            } else {
                Map<String, Object> m = new LinkedHashMap<>();
                if (!n.attrs.isEmpty()) m.put("attr", n.attrs);
                if (!n.children.isEmpty()) m.putAll(n.children);
                String t = n.text.toString().trim();
                if (!t.isEmpty()) m.put("text", t);
                value = m;
            }

            if (!stack.isEmpty()) {
                putMulti(stack.peek().children, n.name, value);
            } else {
                putMulti(result, n.name, value);
            }
        }

        @SuppressWarnings("unchecked")
        private static void putMulti(Map<String, Object> map, String name, Object value) {
            Object existing = map.get(name);
            if (existing == null) {
                map.put(name, value);                       // 1 occurrence → scalar
            } else if (existing instanceof List<?> list) {
                ((List<Object>) list).add(value);           // already a list → append
            } else {
                List<Object> list = new ArrayList<>();      // 2nd occurrence → listify
                list.add(existing);
                list.add(value);
                map.put(name, list);
            }
        }
    }
}
