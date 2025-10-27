package com.hcltech.rmg.common.azure_blob_storage;

import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.net.URI;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class AzureBlobConfigTest {

    @Nested
    class ValidationTests {
        @Test
        void nullsAreRejected() {
            assertThrows(NullPointerException.class,
                    () -> new AzureBlobConfig(null, "c", "p.csv", "sv=...&sig=...", null));
            assertThrows(NullPointerException.class,
                    () -> new AzureBlobConfig("acct", null, "p.csv", "sv=...&sig=...", null));
            assertThrows(NullPointerException.class,
                    () -> new AzureBlobConfig("acct", "c", null, "sv=...&sig=...", null));
            assertThrows(NullPointerException.class,
                    () -> new AzureBlobConfig("acct", "c", "p.csv", null, null));
        }

        @Test
        void blanksAreRejected() {
            assertThrows(IllegalArgumentException.class,
                    () -> new AzureBlobConfig("", "c", "p.csv", "sv=...&sig=...", null));
            assertThrows(IllegalArgumentException.class,
                    () -> new AzureBlobConfig("acct", "", "p.csv", "sv=...&sig=...", null));
            assertThrows(IllegalArgumentException.class,
                    () -> new AzureBlobConfig("acct", "c", "", "sv=...&sig=...", null));
            assertThrows(IllegalArgumentException.class,
                    () -> new AzureBlobConfig("acct", "c", "p.csv", "", null));
        }
    }

    @Nested
    class EndpointHostTests {
        @Test
        void defaultEndpointHostIsPublicAzure() {
            AzureBlobConfig cfg = new AzureBlobConfig("acct", "data", "x.csv", "sv=1&sig=abc", null);
            assertEquals("blob.core.windows.net", cfg.resolvedEndpointHost());
        }

        @Test
        void customEndpointHostOverridesDefault() {
            AzureBlobConfig cfg = new AzureBlobConfig("acct", "data", "x.csv", "sv=1&sig=abc", "127.0.0.1:10000");
            assertEquals("127.0.0.1:10000", cfg.resolvedEndpointHost());
        }
    }

    @Nested
    class SignedUriTests {
        @Test
        void buildsSignedUri_withLeadingQuestionMarkInSas() {
            AzureBlobConfig cfg = new AzureBlobConfig(
                    "myacct", "data", "lookups/countries.csv",
                    "?sv=2024-05-04&sp=rl&se=2026-01-01&sig=XYZ",
                    null);

            URI uri = cfg.signedBlobUri();
            assertEquals(
                    "https://myacct.blob.core.windows.net/data/lookups/countries.csv?sv=2024-05-04&sp=rl&se=2026-01-01&sig=XYZ",
                    uri.toString());
        }

        @Test
        void buildsSignedUri_withoutLeadingQuestionMarkInSas() {
            AzureBlobConfig cfg = new AzureBlobConfig(
                    "myacct", "data", "lookups/countries.csv",
                    "sv=2024-05-04&sp=rl&se=2026-01-01&sig=XYZ",
                    null);

            URI uri = cfg.signedBlobUri();
            assertEquals(
                    "https://myacct.blob.core.windows.net/data/lookups/countries.csv?sv=2024-05-04&sp=rl&se=2026-01-01&sig=XYZ",
                    uri.toString());
        }

        @Test
        void normalizesBlobPathThatStartsWithSlash() {
            AzureBlobConfig cfg = new AzureBlobConfig(
                    "acct", "data", "/lookups/cities.csv",
                    "sv=1&sig=abc",
                    null);

            assertEquals(
                    "https://acct.blob.core.windows.net/data/lookups/cities.csv?sv=1&sig=abc",
                    cfg.signedBlobUri().toString());
        }

        @Test
        void preservesBlobPathWithoutSlash() {
            AzureBlobConfig cfg = new AzureBlobConfig(
                    "acct", "data", "lookups/cities.csv",
                    "sv=1&sig=abc",
                    null);

            assertEquals(
                    "https://acct.blob.core.windows.net/data/lookups/cities.csv?sv=1&sig=abc",
                    cfg.signedBlobUri().toString());
        }

        @Test
        void worksWithCustomEndpointHost_e_g_Azurite() {
            AzureBlobConfig cfg = new AzureBlobConfig(
                    "devstoreaccount1", "data", "x.csv",
                    "sv=1&sig=abc",
                    "127.0.0.1:10000");

            assertEquals(
                    "https://devstoreaccount1.127.0.0.1:10000/data/x.csv?sv=1&sig=abc",
                    cfg.signedBlobUri().toString());
        }
    }
}
