package com.hcltech.rmg.common;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class IEnvGetterTest {

    /** Simple map-backed env for tests. */
    static class MapEnv implements IEnvGetter {
        private final Map<String, String> map;
        MapEnv(Map<String, String> map) { this.map = map; }
        @Override public String get(String name) { return map.get(name); }
    }

    private static MapEnv env(Map<String, String> kv) {
        return new MapEnv(new HashMap<>(kv));
    }

    // ---------------------------------------------------------------------
    // Required getters
    // ---------------------------------------------------------------------

    @Nested
    @DisplayName("getString (required)")
    class GetStringRequired {
        @Test
        void returnsValueWhenPresent() {
            var e = env(Map.of("X", "abc"));
            assertEquals("abc", IEnvGetter.getString(e, "X"));
        }

        @Test
        void throwsWhenMissing() {
            var e = env(Map.of());
            var ex = assertThrows(IllegalStateException.class, () -> IEnvGetter.getString(e, "X"));
            assertTrue(ex.getMessage().contains("Missing required environment variable: X"));
        }

        @Test
        void throwsWhenBlank() {
            var e = env(Map.of("X", "   "));
            assertThrows(IllegalStateException.class, () -> IEnvGetter.getString(e, "X"));
        }
    }

    @Nested
    @DisplayName("getBoolean (required)")
    class GetBooleanRequired {
        @Test
        void parsesTrueCaseInsensitive() {
            var e = env(Map.of("FLAG", "TrUe"));
            assertTrue(IEnvGetter.getBoolean(e, "FLAG"));
        }

        @Test
        void parsesFalseForNonTrueValues() {
            var e = env(Map.of("FLAG", "yes"));
            assertFalse(IEnvGetter.getBoolean(e, "FLAG"));
        }

        @Test
        void throwsWhenMissingOrBlank() {
            assertThrows(IllegalStateException.class, () -> IEnvGetter.getBoolean(env(Map.of()), "FLAG"));
            assertThrows(IllegalStateException.class, () -> IEnvGetter.getBoolean(env(Map.of("FLAG", "   ")), "FLAG"));
        }
    }

    @Nested
    @DisplayName("getInt (required)")
    class GetIntRequired {
        @Test
        void parsesValidInt() {
            var e = env(Map.of("N", "42"));
            assertEquals(42, IEnvGetter.getInt(e, "N"));
        }

        @Test
        void trimsBeforeParse() {
            var e = env(Map.of("N", "  7 "));
            assertEquals(7, IEnvGetter.getInt(e, "N"));
        }

        @Test
        void throwsOnMissingBlankOrInvalid() {
            assertThrows(IllegalStateException.class, () -> IEnvGetter.getInt(env(Map.of()), "N"));
            assertThrows(IllegalStateException.class, () -> IEnvGetter.getInt(env(Map.of("N", " ")), "N"));

            var ex = assertThrows(IllegalStateException.class, () -> IEnvGetter.getInt(env(Map.of("N", "abc")), "N"));
            assertTrue(ex.getMessage().contains("Invalid integer"));
        }
    }

    @Nested
    @DisplayName("getLong (required)")
    class GetLongRequired {
        @Test
        void parsesValidLong() {
            var e = env(Map.of("L", "9223372036854775806"));
            assertEquals(9223372036854775806L, IEnvGetter.getLong(e, "L"));
        }

        @Test
        void trimsBeforeParse() {
            var e = env(Map.of("L", "  10 "));
            assertEquals(10L, IEnvGetter.getLong(e, "L"));
        }

        @Test
        void throwsOnMissingBlankOrInvalid() {
            assertThrows(IllegalStateException.class, () -> IEnvGetter.getLong(env(Map.of()), "L"));
            assertThrows(IllegalStateException.class, () -> IEnvGetter.getLong(env(Map.of("L", "")), "L"));

            var ex = assertThrows(IllegalStateException.class, () -> IEnvGetter.getLong(env(Map.of("L", "big")), "L"));
            assertTrue(ex.getMessage().contains("Invalid long"));
        }
    }

    // ---------------------------------------------------------------------
    // Optional getters
    // ---------------------------------------------------------------------

    @Nested
    @DisplayName("getStringOr (optional)")
    class GetStringOrTests {
        @Test
        void returnsDefaultWhenMissingOrBlank() {
            assertEquals("def", IEnvGetter.getStringOr(env(Map.of()), "X", "def"));
            assertEquals("def", IEnvGetter.getStringOr(env(Map.of("X", "   ")), "X", "def"));
        }

        @Test
        void returnsValueWhenPresent() {
            assertEquals("abc", IEnvGetter.getStringOr(env(Map.of("X", "abc")), "X", "def"));
        }
    }

    @Nested
    @DisplayName("getBooleanOr (optional)")
    class GetBooleanOrTests {
        @Test
        void defaultWhenMissingOrBlank() {
            assertTrue(IEnvGetter.getBooleanOr(env(Map.of()), "FLAG", true));
            assertFalse(IEnvGetter.getBooleanOr(env(Map.of()), "FLAG", false));
            assertTrue(IEnvGetter.getBooleanOr(env(Map.of("FLAG", "  ")), "FLAG", true));
        }

        @Test
        void parsesWhenPresent() {
            assertTrue(IEnvGetter.getBooleanOr(env(Map.of("FLAG", "true")), "FLAG", false));
            assertFalse(IEnvGetter.getBooleanOr(env(Map.of("FLAG", "yes")), "FLAG", true));
        }
    }

    @Nested
    @DisplayName("getIntOr (optional)")
    class GetIntOrTests {
        @Test
        void defaultWhenMissingOrBlank() {
            assertEquals(7, IEnvGetter.getIntOr(env(Map.of()), "N", 7));
            assertEquals(7, IEnvGetter.getIntOr(env(Map.of("N", " ")), "N", 7));
        }

        @Test
        void parsesValidInt() {
            assertEquals(42, IEnvGetter.getIntOr(env(Map.of("N", "42")), "N", 0));
            assertEquals(5, IEnvGetter.getIntOr(env(Map.of("N", " 5 ")), "N", 0));
        }

        @Test
        void invalidIntThrowsIllegalStateException() {
            var ex = assertThrows(IllegalStateException.class,
                    () -> IEnvGetter.getIntOr(env(Map.of("N", "abc")), "N", 0));
            assertTrue(ex.getMessage().contains("Invalid integer"));
        }
    }

    @Nested
    @DisplayName("getLongOr (optional)")
    class GetLongOrTests {
        @Test
        void defaultWhenMissingOrBlank() {
            assertEquals(9L, IEnvGetter.getLongOr(env(Map.of()), "L", 9L));
            assertEquals(9L, IEnvGetter.getLongOr(env(Map.of("L", "")), "L", 9L));
        }

        @Test
        void parsesValidLong() {
            assertEquals(10L, IEnvGetter.getLongOr(env(Map.of("L", "10")), "L", 0L));
            assertEquals(11L, IEnvGetter.getLongOr(env(Map.of("L", " 11 ")), "L", 0L));
        }

        @Test
        void invalidLongThrowsIllegalStateException() {
            var ex = assertThrows(IllegalStateException.class,
                    () -> IEnvGetter.getLongOr(env(Map.of("L", "big")), "L", 0L));
            assertTrue(ex.getMessage().contains("Invalid long"));
        }
    }

    // ---------------------------------------------------------------------
    // Real environment test
    // ---------------------------------------------------------------------

    @Test
    @DisplayName("System::getenv-backed IEnvGetter.env returns real environment values")
    void realEnvGetterWorks() {
        IEnvGetter real = IEnvGetter.env;

        // Check at least one of PATH, HOME, or USER is non-null and non-empty
        String path = real.get("PATH");
        String home = real.get("HOME");
        String user = real.get("USER");

        assertTrue(
                (path != null && !path.isBlank()) ||
                        (home != null && !home.isBlank()) ||
                        (user != null && !user.isBlank()),
                "Expected at least one common environment variable (PATH, HOME, USER) to be present"
        );
    }
}
