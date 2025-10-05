package com.hcltech.rmg.woodstox;

import org.codehaus.stax2.XMLStreamReader2;

import java.util.*;

/**
 * Streaming builder producing a CEL-friendly deterministic Map<String,Object>.
 *
 * Shape:
 *   - Element keys: sanitized to CEL identifiers ([A-Za-z_][A-Za-z0-9_]*).
 *   - Attributes: nested under "attr" sub-map with sanitized keys.
 *   - Text: stored under "text".
 *   - Repeats: always lists (first repeat converts scalar to List).
 *   - Order: LinkedHashMap for stable iteration.
 *
 * Notes:
 *   - Uses local names; namespaces are ignored to keep keys CEL-safe. If you need namespaces later,
 *     introduce a prefixing policy and still sanitize to CEL-safe keys.
 */
final class CelFriendlyStreamingMapBuilder {

    private static final String ATTR = "attr";
    private static final String TEXT = "text";

    // Stack of current element maps; we keep a synthetic root holder for convenience
    private final Deque<Map<String, Object>> stack = new ArrayDeque<>();
    private final Deque<String> keyStack = new ArrayDeque<>();
    private final StringBuilder textBuf = new StringBuilder();

    private Map<String, Object> root;         // final result root map
    private Map<String, Object> currentNode;  // convenience pointer

    /** Convert an XML local name to a CEL-safe identifier. */
    private static String celId(String raw) {
        if (raw == null || raw.isEmpty()) return "_";
        StringBuilder sb = new StringBuilder(raw.length());
        char c0 = raw.charAt(0);
        if (Character.isLetter(c0) || c0 == '_') {
            sb.append(c0);
        } else {
            sb.append('_');
            if (Character.isLetterOrDigit(c0)) {
                sb.append(c0);
            }
        }
        for (int i = 1; i < raw.length(); i++) {
            char c = raw.charAt(i);
            sb.append((Character.isLetterOrDigit(c) || c == '_') ? c : '_');
        }
        return sb.toString();
    }

    /** Attach a new child node under the given key in the current parent (create root if needed). */
    private void attachToParent(String key, Map<String, Object> child) {
        if (stack.isEmpty()) {
            // Create a holder map as the true root and then put the element under its sanitized key
            root = new LinkedHashMap<>();
            root.put(key, child);
            // Push holder so siblings attach correctly
            stack.push(root);
            keyStack.push("<root>");
            return;
        }
        Map<String, Object> parent = stack.peek();
        Object prev = parent.get(key);
        if (prev == null) {
            parent.put(key, child);
        } else if (prev instanceof List) {
            ((List<Object>) prev).add(child);
        } else {
            List<Object> list = new ArrayList<>();
            list.add(prev);
            list.add(child);
            parent.put(key, list);
        }
    }

    /** START_ELEMENT handler. */
    void onStart(XMLStreamReader2 r) {
        // finalize any accumulated inter-element text for the current node
        flushTextToCurrent();

        String elemKey = celId(r.getLocalName());
        Map<String, Object> node = new LinkedHashMap<>();

        // attributes → nested sub-map "attr"
        int ac = r.getAttributeCount();
        if (ac > 0) {
            Map<String, Object> attrs = new LinkedHashMap<>(ac * 2);
            for (int i = 0; i < ac; i++) {
                String aKey = celId(r.getAttributeLocalName(i));
                attrs.put(aKey, r.getAttributeValue(i));
            }
            node.put(ATTR, attrs);
        }

        attachToParent(elemKey, node);

        // push new current element
        stack.push(node);
        keyStack.push(elemKey);
        currentNode = node;
        textBuf.setLength(0);
    }

    /** CHARACTER data handler. */
    void onText(XMLStreamReader2 r) {
        if (!r.isWhiteSpace()) {
            textBuf.append(r.getTextCharacters(), r.getTextStart(), r.getTextLength());
        }
    }

    /** END_ELEMENT handler. */
    void onEnd(XMLStreamReader2 r) {
        // on element close, write accumulated text (if any)
        if (textBuf.length() > 0) {
            // If node already has children or attributes, put text under "text"
            currentNode.put(TEXT, textBuf.toString());
            textBuf.setLength(0);
        }
        // pop element
        stack.pop();
        keyStack.pop();
        currentNode = stack.isEmpty() ? null : stack.peek();
    }

    /** Final result root map (never null; may be empty). */
    Map<String, Object> result() {
        return root != null ? root : Map.of();
    }

    /** If there was text between siblings at the same level, write it to the current node. */
    private void flushTextToCurrent() {
        if (textBuf.length() > 0 && currentNode != null) {
            currentNode.put(TEXT, textBuf.toString());
            textBuf.setLength(0);
        }
    }
}
