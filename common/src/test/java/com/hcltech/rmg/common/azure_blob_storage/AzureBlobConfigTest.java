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
                    () -> new AzureBlobConfig(null, "c", "p.csv", null, null));
            assertThrows(NullPointerException.class,
                    () -> new AzureBlobConfig("acct", null, "p.csv", null, null));
            assertThrows(NullPointerException.class,
                    () -> new AzureBlobConfig("acct", "c", null, null, null));
        }

        @Test
        void blanksAreRejected_forRequiredFields_only() {
            assertThrows(IllegalArgumentException.class,
                    () -> new AzureBlobConfig("", "c", "p.csv", null, null));
            assertThrows(IllegalArgumentException.class,
                    () -> new AzureBlobConfig("acct", "", "p.csv", null, null));
            assertThrows(IllegalArgumentException.class,
                    () -> new AzureBlobConfig("acct", "c", "", null, null));
        }
    }

    @Nested
    class EndpointHostTests {
        @Test
        void defaultEndpointHostIsPublicAzure() {
            AzureBlobConfig cfg = new AzureBlobConfig(
                    "acct", "data", "x.csv",
                    null, null);
            assertEquals("blob.core.windows.net", cfg.resolvedEndpointHost());
        }

        @Test
        void customEndpointHostOverridesDefault_hostStyle() {
            AzureBlobConfig cfg = new AzureBlobConfig(
                    "acct", "data", "x.csv",
                    null, "custom.blob.endpoint");
            assertEquals("custom.blob.endpoint", cfg.resolvedEndpointHost());
        }

        @Test
        void customEndpointHost_canBeAFullBaseUrl_pathStyle() {
            AzureBlobConfig cfg = new AzureBlobConfig(
                    "acct", "data", "x.csv",
                    null, "http://127.0.0.1:10000");
            assertEquals("http://127.0.0.1:10000", cfg.resolvedEndpointHost());
        }
    }

    @Nested
    class SasResolutionTests {
        @Test
        void resolvedSas_readsFromEnvVar_whenProvided() {
            IEnvGetter fakeEnv = name -> name.equals("AZ_SAS") ? "sv=envsig" : null;

            AzureBlobConfig cfg = new AzureBlobConfig(
                    "acct", "data", "f.csv",
                    "AZ_SAS",
                    null);

            assertEquals(Optional.of("?sv=envsig"), cfg.resolvedSas(fakeEnv));
        }

        @Test
        void resolvedSas_emptyWhenNothingProvided() {
            AzureBlobConfig cfg = new AzureBlobConfig(
                    "acct", "data", "f.csv",
                    null, null);
            IEnvGetter emptyEnv = name -> null;

            assertEquals(Optional.empty(), cfg.resolvedSas(emptyEnv));
        }
    }

    @Nested
    class BlobUriTests {
        @Test
        void blobUri_buildsCorrectUrl_hostStyle() {
            AzureBlobConfig cfg = new AzureBlobConfig(
                    "acct", "data", "lookups/cities.csv",
                    null, null);
            URI uri = cfg.blobUri();
            assertEquals("https://acct.blob.core.windows.net/data/lookups/cities.csv", uri.toString());
        }

        @Test
        void blobUri_normalizesLeadingSlash() {
            AzureBlobConfig cfg = new AzureBlobConfig(
                    "acct", "data", "/lookups/cities.csv",
                    null, null);
            URI uri = cfg.blobUri();
            assertEquals("https://acct.blob.core.windows.net/data/lookups/cities.csv", uri.toString());
        }

        @Test
        void blobUri_usesCustomEndpointHost_hostStyle() {
            AzureBlobConfig cfg = new AzureBlobConfig(
                    "devstoreaccount1", "data", "x.csv",
                    null, "custom.endpoint");
            URI uri = cfg.blobUri();
            assertEquals("https://devstoreaccount1.custom.endpoint/data/x.csv", uri.toString());
        }

        @Test
        void blobUri_supportsPathStyle_whenEndpointIsFullBaseUrl() {
            AzureBlobConfig cfg = new AzureBlobConfig(
                    "devstoreaccount1", "data", "x.csv",
                    null, "http://127.0.0.1:10000");
            URI uri = cfg.blobUri();
            assertEquals("http://127.0.0.1:10000/devstoreaccount1/data/x.csv", uri.toString());
        }

        @Test
        void blobUri_encodesSpacesAndWeirdChars_inBlobPathSegments() {
            AzureBlobConfig cfg = new AzureBlobConfig(
                    "acct", "data", "folder with space/file+plus#.csv",
                    null, null);
            URI uri = cfg.blobUri();
            // Expect each segment encoded; spaces as %20, + encoded as %2B, # as %23
            assertEquals("https://acct.blob.core.windows.net/data/folder%20with%20space/file%2Bplus%23.csv", uri.toString());
        }
    }

    @Nested
    class BlobUriWithSasTests {
        @Test
        void blobUriWithSas_appendsLeadingQuestionMarkIfMissing() {
            IEnvGetter env = name -> name.equals("MY_SAS") ? "sv=abc&sig=xyz" : null;

            AzureBlobConfig cfg = new AzureBlobConfig(
                    "acct", "data", "file.csv",
                    "MY_SAS",
                    null);

            URI uri = cfg.blobUriWithSas(env);
            assertEquals("https://acct.blob.core.windows.net/data/file.csv?sv=abc&sig=xyz", uri.toString());
        }

        @Test
        void blobUriWithSas_noAppend_whenNoSasPresent() {
            AzureBlobConfig cfg = new AzureBlobConfig(
                    "acct", "data", "file.csv",
                    null, null);

            IEnvGetter env = name -> null;
            URI uri = cfg.blobUriWithSas(env);
            assertEquals("https://acct.blob.core.windows.net/data/file.csv", uri.toString());
        }
    }
}
