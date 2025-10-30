package com.hcltech.rmg.parameters;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.*;

class ParametersTest {

    // ---- defaultKeyFn -------------------------------------------------------

    @Test
    @DisplayName("defaultKeyFn: joins values by colon")
    void defaultKeyFn_joinsByColon() {
        assertEquals("dev:uk", Parameters.defaultKeyFn(List.of("dev", "uk")));
        assertEquals("a:b:c", Parameters.defaultKeyFn(List.of("a", "b", "c")));
    }

    @Test
    @DisplayName("defaultKeyFn: empty list yields empty string")
    void defaultKeyFn_emptyList() {
        assertEquals("", Parameters.defaultKeyFn(List.of()));
    }

    // ---- defaultResourceFn --------------------------------------------------

    @Test
    @DisplayName("defaultResourceFn: uses prefix as-is (no slash insertion)")
    void defaultResourceFn_noSlashInsertion() {
        Function<List<String>, String> res = Parameters.defaultResourceFn("config/test");
        // NOTE: current implementation does NOT insert '/' after prefix
        assertEquals("config/testdev/uk.json", res.apply(List.of("dev", "uk")));
    }

    @Test
    @DisplayName("defaultResourceFn: respects trailing slash when present")
    void defaultResourceFn_respectsTrailingSlash() {
        Function<List<String>, String> res = Parameters.defaultResourceFn("config/test/");
        assertEquals("config/test/dev/uk.json", res.apply(List.of("dev", "uk")));
    }

    @Test
    @DisplayName("defaultResourceFn: empty values list produces prefix + .json")
    void defaultResourceFn_emptyValuesList() {
        Function<List<String>, String> res1 = Parameters.defaultResourceFn("config/test");
        assertEquals("config/test.json", res1.apply(List.of()));

        Function<List<String>, String> res2 = Parameters.defaultResourceFn("config/test/");
        assertEquals("config/test/.json", res2.apply(List.of())); // trailing slash is respected
    }

    // ---- constructor validation --------------------------------------------

    @Test
    @DisplayName("constructor: name/value length mismatch throws")
    void constructor_mismatch_throws() {
        List<String> names = List.of("env", "country");
        List<String> values = List.of("dev"); // mismatch

        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class, () ->
                new Parameters(names, values, "ignoredKey", "parcel", "D1", "Arrived")
        );
        assertTrue(ex.getMessage().toLowerCase().contains("same size"));
    }

    // ---- of(...) convenience ------------------------------------------------

    @Test
    @DisplayName("of(): builds key from values via defaultKeyFn and sets domain fields")
    void of_buildsKey_and_setsDomainFields() {
        List<String> names = List.of("env", "country");
        List<String> values = List.of("dev", "uk");

        Parameters p = Parameters.of(names, values, "parcel", "D-123", "Arrived");

        assertEquals(names, p.parameterNames());
        assertEquals(values, p.parameterValues());
        assertEquals("dev:uk", p.key(), "key should be built with defaultKeyFn (colon-joined)");
        assertEquals("parcel", p.domainType());
        assertEquals("D-123", p.domainId());
        assertEquals("Arrived", p.eventType());
    }
}
