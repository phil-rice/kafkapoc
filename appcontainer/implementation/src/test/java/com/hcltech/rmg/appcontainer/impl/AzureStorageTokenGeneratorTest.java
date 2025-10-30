package com.hcltech.rmg.appcontainer.impl;

import com.azure.core.credential.AccessToken;
import com.azure.core.credential.TokenCredential;
import com.azure.core.credential.TokenRequestContext;
import com.hcltech.rmg.common.IEnvGetter;
import com.hcltech.rmg.common.tokens.Token;
import com.hcltech.rmg.common.tokens.Token.Type;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import reactor.core.publisher.Mono;

import java.time.OffsetDateTime;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class AzureStorageTokenGeneratorTest {

    // ===== Helpers =====

    private static AccessToken accessToken(String val) {
        return new AccessToken(val, OffsetDateTime.now().plusHours(1));
    }

    private static final String SCOPE = "https://storage.azure.com/.default";

    // ===== Tests: SAS path =====

    @Test
    void returnsSasToken_whenEnvVarPresent_withLeadingQuestionMarkPreserved() {
        TokenCredential cred = mock(TokenCredential.class); // must not be called
        IEnvGetter env = name -> name.equals("LOCAL_BLOB_SAS") ? "?sv=abc&sig=xyz" : null;
        AzureStorageTokenGenerator gen = new AzureStorageTokenGenerator(cred, env);

        Token t = gen.token("LOCAL_BLOB_SAS");

        assertEquals(Type.SAS, t.type());
        assertEquals("?sv=abc&sig=xyz", t.value());
        verifyNoInteractions(cred);
    }

    @Test
    void returnsSasToken_whenEnvVarPresent_withoutLeadingQuestionMark_normalized() {
        TokenCredential cred = mock(TokenCredential.class); // must not be called
        IEnvGetter env = name -> name.equals("LOCAL_BLOB_SAS") ? "sv=abc&sig=xyz" : null;
        AzureStorageTokenGenerator gen = new AzureStorageTokenGenerator(cred, env);

        Token t = gen.token("LOCAL_BLOB_SAS");

        assertEquals(Type.SAS, t.type());
        assertEquals("?sv=abc&sig=xyz", t.value(), "SAS should be normalized to start with '?'");
        verifyNoInteractions(cred);
    }

    @Test
    void doesNotCallCredential_whenSasPresent_evenIfCredentialConfigured() {
        TokenCredential cred = mock(TokenCredential.class);
        when(cred.getToken(any())).thenReturn(Mono.just(accessToken("should-not-be-used")));
        IEnvGetter env = name -> "sv=abc&sig=xyz"; // any name resolves to SAS
        AzureStorageTokenGenerator gen = new AzureStorageTokenGenerator(cred, env);

        Token t = gen.token("ANY_ENV_NAME");

        assertEquals(Type.SAS, t.type());
        assertEquals("?sv=abc&sig=xyz", t.value());
        verifyNoInteractions(cred);
    }

    @Test
    void blankEnvValue_triggersBearerFallback() {
        TokenCredential cred = mock(TokenCredential.class);
        when(cred.getToken(any())).thenReturn(Mono.just(accessToken("jwt.blank-env")));
        IEnvGetter env = name -> "  "; // blank value returned
        AzureStorageTokenGenerator gen = new AzureStorageTokenGenerator(cred, env);

        Token t = gen.token("LOCAL_BLOB_SAS");

        assertEquals(Type.BEARER, t.type());
        assertTrue(t.value().startsWith("Bearer "));
        verify(cred, times(1)).getToken(any());
    }

    // ===== Tests: Bearer fallback path =====

    @Test
    void fallsBackToBearer_whenEnvVarMissing_callsCredential_andPrefixesBearer_andUsesStorageScope() {
        TokenCredential cred = mock(TokenCredential.class);
        when(cred.getToken(any(TokenRequestContext.class))).thenReturn(Mono.just(accessToken("mock.jwt.token")));
        IEnvGetter env = name -> null; // no env set
        AzureStorageTokenGenerator gen = new AzureStorageTokenGenerator(cred, env);

        ArgumentCaptor<TokenRequestContext> ctxCap = ArgumentCaptor.forClass(TokenRequestContext.class);

        Token t = gen.token("LOCAL_BLOB_SAS");

        assertEquals(Type.BEARER, t.type());
        assertTrue(t.value().startsWith("Bearer "), "Bearer token must be prefixed with 'Bearer '");
        assertTrue(t.value().contains("mock.jwt.token"));

        verify(cred, times(1)).getToken(ctxCap.capture());
        List<String> scopes = ctxCap.getValue().getScopes();
        assertEquals(1, scopes.size());
        assertEquals(SCOPE, scopes.get(0));
    }

    @Test
    void fallsBackToBearer_whenEnvVarNameIsNull() {
        TokenCredential cred = mock(TokenCredential.class);
        when(cred.getToken(any())).thenReturn(Mono.just(accessToken("jwt.null")));
        IEnvGetter env = name -> "should-not-be-called";
        AzureStorageTokenGenerator gen = new AzureStorageTokenGenerator(cred, env);

        Token t = gen.token(null);

        assertEquals(Type.BEARER, t.type());
        assertTrue(t.value().startsWith("Bearer "));
        verify(cred, times(1)).getToken(any());
    }

    @Test
    void fallsBackToBearer_whenEnvVarNameIsBlank() {
        TokenCredential cred = mock(TokenCredential.class);
        when(cred.getToken(any())).thenReturn(Mono.just(accessToken("jwt.blank-name")));
        IEnvGetter env = name -> "should-not-be-called";
        AzureStorageTokenGenerator gen = new AzureStorageTokenGenerator(cred, env);

        Token t = gen.token("   ");

        assertEquals(Type.BEARER, t.type());
        assertTrue(t.value().startsWith("Bearer "));
        verify(cred, times(1)).getToken(any());
    }

    @Test
    void credentialError_bubblesUp() {
        TokenCredential cred = mock(TokenCredential.class);
        when(cred.getToken(any())).thenReturn(Mono.error(new RuntimeException("auth failed")));
        IEnvGetter env = name -> null; // force bearer path
        AzureStorageTokenGenerator gen = new AzureStorageTokenGenerator(cred, env);

        RuntimeException ex = assertThrows(RuntimeException.class, () -> gen.token(null));
        assertTrue(ex.getMessage().contains("auth failed"));
        verify(cred, times(1)).getToken(any());
    }
}
