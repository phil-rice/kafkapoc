package com.hcltech.rmg.common;

/**
 * Abstraction for reading environment variables or configuration values.
 * <p>
 * Used to avoid direct calls to {@link System#getenv(String)} in code,
 * so that unit tests can provide their own environment source.
 */
@FunctionalInterface
public interface IEnvGetter {
    /**
     * Default implementation backed by {@link System#getenv(String)}.
     */
    IEnvGetter env = System::getenv;
    /**
     * Returns the value of the given environment variable, or {@code null} if unset.
     */
    String get(String name);

    // ------------------------------------------------------------------------
    // Required getters (throw if missing or blank)
    // ------------------------------------------------------------------------

    /**
     * Returns the value of the environment variable, throwing if missing or blank.
     */
    static String getString(IEnvGetter env, String name) {
        String value = env.get(name);
        if (value == null || value.isBlank()) {
            throw new IllegalStateException("Missing required environment variable: " + name);
        }
        return value;
    }

    /**
     * Returns the boolean value of the environment variable, throwing if missing or blank.
     * Only the case-insensitive string "true" is considered true; everything else is false.
     */
    static boolean getBoolean(IEnvGetter env, String name) {
        String value = env.get(name);
        if (value == null || value.isBlank()) {
            throw new IllegalStateException("Missing required environment variable: " + name);
        }
        return Boolean.parseBoolean(value);
    }

    /**
     * Returns the integer value of the environment variable, throwing if missing, blank, or invalid.
     */
    static int getInt(IEnvGetter env, String name) {
        String value = env.get(name);
        if (value == null || value.isBlank()) {
            throw new IllegalStateException("Missing required environment variable: " + name);
        }
        try {
            return Integer.parseInt(value.trim());
        } catch (NumberFormatException e) {
            throw new IllegalStateException("Invalid integer for environment variable: " + name + " = '" + value + "'", e);
        }
    }

    /**
     * Returns the long value of the environment variable, throwing if missing, blank, or invalid.
     */
    static long getLong(IEnvGetter env, String name) {
        String value = env.get(name);
        if (value == null || value.isBlank()) {
            throw new IllegalStateException("Missing required environment variable: " + name);
        }
        try {
            return Long.parseLong(value.trim());
        } catch (NumberFormatException e) {
            throw new IllegalStateException("Invalid long for environment variable: " + name + " = '" + value + "'", e);
        }
    }

    // ------------------------------------------------------------------------
    // Optional getters (default fallback)
    // ------------------------------------------------------------------------

    static String getStringOr(IEnvGetter env, String name, String defaultValue) {
        String value = env.get(name);
        return (value != null && !value.isBlank()) ? value : defaultValue;
    }

    static boolean getBooleanOr(IEnvGetter env, String name, boolean defaultValue) {
        String value = env.get(name);
        return (value != null && !value.isBlank()) ? Boolean.parseBoolean(value) : defaultValue;
    }

    static int getIntOr(IEnvGetter env, String name, int defaultValue) {
        String value = env.get(name);
        if (value == null || value.isBlank()) return defaultValue;
        try {
            return Integer.parseInt(value.trim());
        } catch (NumberFormatException e) {
            throw new IllegalStateException("Invalid integer for environment variable: " + name + " = '" + value + "'", e);
        }
    }

    static long getLongOr(IEnvGetter env, String name, long defaultValue) {
        String value = env.get(name);
        if (value == null || value.isBlank()) return defaultValue;
        try {
            return Long.parseLong(value.trim());
        } catch (NumberFormatException e) {
            throw new IllegalStateException("Invalid long for environment variable: " + name + " = '" + value + "'", e);
        }
    }


}
