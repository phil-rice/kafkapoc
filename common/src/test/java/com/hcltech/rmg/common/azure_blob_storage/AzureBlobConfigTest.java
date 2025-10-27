package com.hcltech.rmg.common.azure_blob_storage;

import com.hcltech.rmg.common.IEnvGetter;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;

class AzureBlobConfigTest {

    @Nested
    class ValidationTests {
        @Test
        void nullsAreRejected_forRequiredFields_only() {
            assertThrows(NullPointerException.class,
                    () -> new AzureBlobConfig(null, "c", "p.csv", null, null, null));
            assertThrows(NullPointerException.class,
                    () -> new AzureBlobConfig("acct", null, "p.csv", null, null, null));
            assertThrows(NullPointerException.class,
                    () -> new AzureBlobConfig("acct", "c", null, null, null, null));
        }

        @Test
        void blanksAreRejected_forRequiredFields_only() {
            assertThrows(IllegalArgumentException.class,
                    () -> new AzureBlobConfig("", "c", "p.csv", null, null, null));
            assertThrows(IllegalArgumentException.class,
                    () -> new AzureBlobConfig("acct", "", "p.csv", null, null, null));
            assertThrows(IllegalArgumentException.class,
                    () -> new AzureBlobConfig("acct", "c", "", null, null, null));
        }
    }

    @Nested
    class EndpointHostTests {
        @Test
        void defaultEndpointHostIsPublicAzure() {
            AzureBlobConfig cfg = new AzureBlobConfig(
                    "acct", "data", "x.csv",
                    null, null, null);
            assertEquals("blob.core.windows.net", cfg.resolvedEndpointHost());
        }

        @Test
        void customEndpointHostOverridesDefault() {
            AzureBlobConfig cfg = new AzureBlobConfig(
                    "acct", "data", "x.csv",
                    null, null, "127.0.0.1:10000");
            assertEquals("127.0.0.1:10000", cfg.resolvedEndpointHost());
        }
    }

    @Nested
    class SasResolutionTests {
        @Test
        void resolvedSas_prefersDirectSasToken() {
            AzureBlobConfig cfg = new AzureBlobConfig(
                    "acct", "data", "f.csv",
                    "sv=1&sig=abc",  // direct SAS provided
                    "ENV_SAS",
                    null);

            // env getter should be ignored because direct SAS exists
            IEnvGetter env = name -> "should_not_be_used";
            assertEquals(Optional.of("?sv=1&sig=abc"), cfg.resolvedSas(env));
        }

        @Test
        void resolvedSas_readsFromEnvVar_whenDirectSasMissing() {
            IEnvGetter fakeEnv = name -> name.equals("AZUREBLOB_SAS_FOR_TEST") ? "sv=envsig" : null;

            AzureBlobConfig cfg = new AzureBlobConfig(
                    "acct", "data", "f.csv",
                    null,                       // no direct SAS
                    "AZUREBLOB_SAS_FOR_TEST",   // env var name
                    null);

            assertEquals(Optional.of("?sv=envsig"), cfg.resolvedSas(fakeEnv));
        }

        @Test
        void resolvedSas_emptyWhenNothingProvided() {
            AzureBlobConfig cfg = new AzureBlobConfig(
                    "acct", "data", "f.csv",
                    null, null, null);
            IEnvGetter emptyEnv = name -> null;

            assertEquals(Optional.empty(), cfg.resolvedSas(emptyEnv));
        }
    }

    @Nested
    class BlobUriTests {
        @Test
        void blobUri_buildsCorrectUrl() {
            AzureBlobConfig cfg = new AzureBlobConfig(
                    "acct", "data", "lookups/cities.csv",
                    null, null, null);
            URI uri = cfg.blobUri();
            assertEquals("https://acct.blob.core.windows.net/data/lookups/cities.csv", uri.toString());
        }

        @Test
        void blobUri_normalizesLeadingSlash() {
            AzureBlobConfig cfg = new AzureBlobConfig(
                    "acct", "data", "/lookups/cities.csv",
                    null, null, null);
            URI uri = cfg.blobUri();
            assertEquals("https://acct.blob.core.windows.net/data/lookups/cities.csv", uri.toString());
        }

        @Test
        void blobUri_usesCustomEndpointHost() {
            AzureBlobConfig cfg = new AzureBlobConfig(
                    "devstoreaccount1", "data", "x.csv",
                    null, null, "127.0.0.1:10000");
            URI uri = cfg.blobUri();
            assertEquals("https://devstoreaccount1.127.0.0.1:10000/data/x.csv", uri.toString());
        }
    }
}
