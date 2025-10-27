package com.hcltech.rmg.appcontainer.impl;

import com.azure.core.credential.TokenCredential;
import com.azure.core.credential.TokenRequestContext;
import com.azure.identity.DefaultAzureCredentialBuilder;
import com.hcltech.rmg.common.IEnvGetter;
import com.hcltech.rmg.common.tokens.ITokenGenerator;
import com.hcltech.rmg.common.tokens.Token;

/**
 * Generates tokens for Azure Storage access.
 * <p>
 * Strategy:
 * <ul>
 *   <li>If a SAS token environment variable name is provided and resolves to a non-empty value via {@link IEnvGetter},
 *       return it as a {@link Token.Type#SAS} (normalized to start with '?').</li>
 *   <li>Otherwise, acquire a Managed Identity bearer token for {@code https://storage.azure.com/.default}
 *       using {@link com.azure.identity.DefaultAzureCredential} and return {@link Token.Type#BEARER}.</li>
 * </ul>
 * <p>
 * Notes:
 * <ul>
 *   <li>Azure Identity caches access tokens in-process; repeated calls are cheap.</li>
 *   <li>Use the 2-arg constructor to inject a user-assigned MI credential or a fake env getter in tests.</li>
 * </ul>
 */
public final class AzureStorageTokenGenerator implements ITokenGenerator {

    private static final String STORAGE_SCOPE = "https://storage.azure.com/.default";

    private final TokenCredential credential;
    private final TokenRequestContext ctx;
    private final IEnvGetter env;

    /** Default: uses DefaultAzureCredential (CLI locally, Managed Identity on App Service) and System.getenv. */
    public AzureStorageTokenGenerator() {
        this(new DefaultAzureCredentialBuilder().build(), System::getenv);
    }

    /** Inject a custom credential and environment getter (useful for tests or user-assigned MI). */
    public AzureStorageTokenGenerator(TokenCredential credential, IEnvGetter envGetter) {
        this.credential = credential;
        this.env = envGetter;
        this.ctx = new TokenRequestContext().addScopes(STORAGE_SCOPE);
    }

    @Override
    public Token token(String envVariableNameOrNull) {
        // 1) Prefer SAS from env
        if (envVariableNameOrNull != null && !envVariableNameOrNull.isBlank()) {
            String sas = env.get(envVariableNameOrNull);
            if (sas != null && !sas.isBlank()) {
                String normalized = sas.startsWith("?") ? sas : "?" + sas;
                return new Token(Token.Type.SAS, normalized);
            }
        }

        // 2) Fall back to Managed Identity / DefaultAzureCredential (Bearer)
        String bearer = credential.getToken(ctx).block().getToken(); // cached by azure-identity
        return new Token(Token.Type.BEARER, "Bearer " + bearer);
    }
}
