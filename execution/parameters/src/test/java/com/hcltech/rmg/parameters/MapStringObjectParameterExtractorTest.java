package com.hcltech.rmg.parameters;

import com.hcltech.rmg.common.Paths;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class MapStringObjectParameterExtractorTest {

    // Helper to build nested maps tersely.
    private static Map<String, Object> obj(Object... kv) {
        Map<String, Object> m = new LinkedHashMap<>();
        for (int i = 0; i < kv.length; i += 2) {
            m.put((String) kv[i], kv[i + 1]);
        }
        return m;
    }

    private static ParameterExtractor<Map<String,Object>> extractor(
            List<String> names,
            Map<String, Map<String, List<String>>> e2p,
            Map<String, List<String>> defaults
    ) {
        return ParameterExtractor.defaultParameterExtractor(names, e2p, defaults);
    }

    private static final String EVT = "OrderCreated";
    private static final String DOMAIN_TYPE = "parcel";
    private static final String DOMAIN_ID = "P-123";

    @Test
    @DisplayName("parameters(): event-specific paths → value")
    void parameters_eventSpecific_success() {
        List<String> names = List.of("id", "user");
        Map<String, Map<String, List<String>>> e2p = Map.of(
                EVT, Map.of(
                        "id", List.of("order", "id"),
                        "user", List.of("user", "name")
                )
        );
        Map<String, List<String>> defaults = Map.of();

        Map<String, Object> input = obj(
                "order", obj("id", "123"),
                "user", obj("name", "Alice")
        );

        Parameters p = assertDoesNotThrow(
                () -> extractor(names, e2p, defaults)
                        .parameters(input, EVT, DOMAIN_TYPE, DOMAIN_ID)
                        .valueOrThrow()
        );
        assertNotNull(p);
        assertEquals(DOMAIN_TYPE, p.domainType());
        assertEquals(DOMAIN_ID, p.domainId());
        assertEquals(EVT, p.eventType());
        assertEquals("123:Alice", p.key()); // names = ["id","user"]
    }

    @Test
    @DisplayName("parameters(): param missing in event → fallback to default path")
    void parameters_fallbackToDefault_whenParamMissingInEvent() {
        List<String> names = List.of("id", "user");
        Map<String, Map<String, List<String>>> e2p = Map.of(
                EVT, Map.of(
                        "id", List.of("order", "id") // 'user' missing here
                )
        );
        Map<String, List<String>> defaults = Map.of(
                "user", List.of("user", "name")
        );

        Map<String, Object> input = obj(
                "order", obj("id", "123"),
                "user", obj("name", "Alice")
        );

        Parameters p = assertDoesNotThrow(
                () -> extractor(names, e2p, defaults)
                        .parameters(input, EVT, DOMAIN_TYPE, DOMAIN_ID)
                        .valueOrThrow()
        );
        assertNotNull(p);
        assertEquals("123:Alice", p.key());
    }

    @Test
    @DisplayName("parameters(): event type missing → use defaults")
    void parameters_usesDefaults_whenEventTypeMissing() {
        List<String> names = List.of("id", "user");
        Map<String, Map<String, List<String>>> e2p = Map.of(); // no event registered
        Map<String, List<String>> defaults = Map.of(
                "id", List.of("default", "id"),
                "user", List.of("user", "name")
        );

        Map<String, Object> input = obj(
                "default", obj("id", "D-99"),
                "user", obj("name", "Alice")
        );

        Parameters p = assertDoesNotThrow(
                () -> extractor(names, e2p, defaults)
                        .parameters(input, EVT, DOMAIN_TYPE, DOMAIN_ID)
                        .valueOrThrow()
        );
        assertNotNull(p);
        assertEquals("D-99:Alice", p.key());
    }

    @Test
    @DisplayName("parameters(): no path configured → exact error message")
    void parameters_noPath_error() {
        List<String> names = List.of("id", "user");
        Map<String, Map<String, List<String>>> e2p = Map.of(
                EVT, Map.of("id", List.of("order", "id"))
        );
        Map<String, List<String>> defaults = Map.of(); // no default for 'user'

        Map<String, Object> input = obj(
                "order", obj("id", "123")
        );

        List<String> errs = extractor(names, e2p, defaults)
                .parameters(input, EVT, DOMAIN_TYPE, DOMAIN_ID)
                .errorsOrThrow();

        assertEquals(1, errs.size());
        assertEquals("No path for parameter user and event type " + EVT, errs.get(0));
    }

    @Test
    @DisplayName("parameters(): path exists but missing value → exact error message")
    void parameters_missingValue_error() {
        List<String> names = List.of("id");
        Map<String, Map<String, List<String>>> e2p = Map.of(
                EVT, Map.of("id", List.of("order", "id"))
        );
        Map<String, List<String>> defaults = Map.of();

        Map<String, Object> input = obj(
                "order", obj() // no 'id'
        );

        List<String> errs = extractor(names, e2p, defaults)
                .parameters(input, EVT, DOMAIN_TYPE, DOMAIN_ID)
                .errorsOrThrow();

        assertEquals(1, errs.size());
        assertEquals("No value for parameter id at path [order, id] and event type " + EVT, errs.get(0));
    }

    @Test
    @DisplayName("parameters(): wrong type at terminal value → exact error message (strict string)")
    void parameters_wrongType_error() {
        List<String> names = List.of("id");
        Map<String, Map<String, List<String>>> e2p = Map.of(
                EVT, Map.of("id", List.of("order", "id"))
        );
        Map<String, List<String>> defaults = Map.of();

        Map<String, Object> input = obj(
                "order", obj("id", 42) // not a String
        );

        List<String> errs = extractor(names, e2p, defaults)
                .parameters(input, EVT, DOMAIN_TYPE, DOMAIN_ID)
                .errorsOrThrow();

        assertEquals(1, errs.size());
        assertEquals("No value for parameter id at path [order, id] and event type " + EVT, errs.get(0));
    }

    @Test
    @DisplayName("parameters(): intermediate non-map → exact error message")
    void parameters_intermediateNonMap_error() {
        List<String> names = List.of("id");
        Map<String, Map<String, List<String>>> e2p = Map.of(
                EVT, Map.of("id", List.of("order", "id"))
        );
        Map<String, List<String>> defaults = Map.of();

        Map<String, Object> input = obj(
                "order", "not-a-map"
        );

        List<String> errs = extractor(names, e2p, defaults)
                .parameters(input, EVT, DOMAIN_TYPE, DOMAIN_ID)
                .errorsOrThrow();

        assertEquals(1, errs.size());
        assertEquals("No value for parameter id at path [order, id] and event type " + EVT, errs.get(0));
    }

    // ------- direct method checks -------

    @Test
    @DisplayName("findPathOrNull: event has param → use event path")
    void findPathOrNull_usesEventPath() {
        MapStringObjectParameterExtractor d = new MapStringObjectParameterExtractor(
                List.of("id"),
                Map.of(EVT, Map.of("id", List.of("order", "id"))),
                Map.of("id", List.of("default", "id"))
        );
        assertEquals(List.of("order", "id"), d.findPathOrNull(EVT, "id"));
    }

    @Test
    @DisplayName("findPathOrNull: param missing in event → fallback to default")
    void findPathOrNull_fallbackToDefault() {
        MapStringObjectParameterExtractor d = new MapStringObjectParameterExtractor(
                List.of("user"),
                Map.of(EVT, Map.of()), // empty per-event map
                Map.of("user", List.of("user", "name"))
        );
        assertEquals(List.of("user", "name"), d.findPathOrNull(EVT, "user"));
    }

    @Test
    @DisplayName("findPathOrNull: neither event nor default has param → null")
    void findPathOrNull_nullWhenNoConfig() {
        MapStringObjectParameterExtractor d = new MapStringObjectParameterExtractor(
                List.of("missing"),
                Map.of(), Map.of()
        );
        assertNull(d.findPathOrNull(EVT, "missing"));
    }

    @Test
    @DisplayName("findValueOrNull: returns value when present and String")
    void findValueOrNull_success() {
        Map<String, Object> input = obj(
                "a", obj("b", obj("c", "v"))
        );
        assertEquals("v", Paths.findStringOrNull(input, List.of("a", "b", "c")));
    }

    @Test
    @DisplayName("findValueOrNull: returns null on wrong type or missing")
    void findValueOrNull_nullOnWrongTypeOrMissing() {
        Map<String, Object> wrongType = obj("a", obj("b", 123));
        assertNull(Paths.findStringOrNull(wrongType, List.of("a", "b")));

        Map<String, Object> missingKey = obj("a", obj()); // no 'b'
        assertNull(Paths.findStringOrNull(missingKey, List.of("a", "b")));

        Map<String, Object> intermediateNotMap = obj("a", "x"); // 'a' is String
        assertNull(Paths.findStringOrNull(intermediateNotMap, List.of("a", "b")));
    }

    @DisplayName("empty parameterNames → empty Parameters and empty key")
    @Test
    void emptyParameterNames_ok() {
        var e = extractor(List.of(), Map.of(), Map.of());
        Parameters p = e.parameters(Map.of("x", "y"), "Evt", "parcel", "P1").valueOrThrow();
        assertTrue(p.parameterNames().isEmpty());
        assertTrue(p.parameterValues().isEmpty());
        assertEquals("", p.key());
    }

    @DisplayName("one missing among many → fail fast with first missing param")
    @Test
    void firstMissingWins() {
        var names = List.of("id", "user", "country");
        var e2p = Map.of("Evt", Map.of(
                "id", List.of("a", "id"),
                "user", List.of("u", "name")
        ));
        var defaults = Map.<String, List<String>>of(); // no path for "country"
        Map<String, Object> input = Map.of("a", Map.of("id", "1"), "u", Map.of("name", "Ann"));
        var errs = extractor(names, e2p, defaults)
                .parameters(input, "Evt", "parcel", "P1").errorsOrThrow();
        assertEquals(List.of("No path for parameter country and event type Evt"), errs);
    }

    @DisplayName("list in the middle → error")
    @Test
    void intermediateList_error() {
        var names = List.of("id");
        var e2p = Map.of("Evt", Map.of("id", List.of("order", "items", "id")));
        var defaults = Map.<String, List<String>>of();
        Map<String, Object> input = Map.of("order", Map.of("items", List.of(Map.of("id", "123"))));
        var errs = extractor(names, e2p, defaults)
                .parameters(input, "Evt", "parcel", "P1").errorsOrThrow();
        assertEquals("No value for parameter id at path [order, items, id] and event type Evt", errs.get(0));
    }

}
