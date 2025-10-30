package com.hcltech.rmg.common.tokens;

/**
 * Abstraction for obtaining an authorization token for Azure (or other) services.
 * <p>
 * Implementations may return, for example:
 * <ul>
 *   <li>A SAS token stored in an environment variable (e.g. "?sv=2024-05-04&ss=b&srt=...&sig=...")</li>
 *   <li>A Bearer token acquired from a Managed Identity on Azure</li>
 * </ul>
 * <p>
 * Example usage:
 * <pre>
 *   ITokenGenerator generator = new AzureStorageTokenGenerator();
 *   String token = generator.token("LOCAL_BLOB_SAS");
 * </pre>
 */
public interface ITokenGenerator {
    /**
     * Returns a token suitable for authenticating to the target service.
     *
     * @param envVariableNameOrNull The name of an environment variable containing a SAS token,
     *                              or {@code null} to use a managed identity.
     * @return The token string (e.g. "?sv=2024-05-04&ss=b&srt=...&sig=..." or "Bearer eyJ0eXAiOiJKV1Qi...")
     */
    Token token(String envVariableNameOrNull);
}
