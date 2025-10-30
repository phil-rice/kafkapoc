package com.hcltech.rmg.common.tokens;

/** 
 * Represents an authorization token returned by an {@link ITokenGenerator}.
 * <p>
 * Typical examples:
 * <ul>
 *   <li>SAS token (e.g. "?sv=2024-05-04&ss=b&srt=...&sig=...")</li>
 *   <li>Bearer token (e.g. "Bearer eyJ0eXAiOiJKV1Qi...")</li>
 * </ul>
 */
public record Token(Type type, String value) {
    public enum Type { SAS, BEARER }
}
