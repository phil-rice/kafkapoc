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
     * Returns the value of the given environment variable, or {@code null} if unset.
     */
    String get(String name);

    IEnvGetter env = System::getenv;
}
