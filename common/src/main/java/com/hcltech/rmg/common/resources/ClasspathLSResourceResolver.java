package com.hcltech.rmg.common.resources;

import org.w3c.dom.ls.LSInput;
import org.w3c.dom.ls.LSResourceResolver;

import java.io.InputStream;
import java.util.Objects;

/**
 * LSResourceResolver that serves XSD includes/imports from the classpath.
 * It understands base/system IDs that start with "classpath:/" and resolves
 * relative schemaLocation paths against them.
 */
public final class ClasspathLSResourceResolver implements LSResourceResolver {
    private static final String SCHEME = "classpath:/";
    private final ClassLoader loader;

    public ClasspathLSResourceResolver() {
        this(Thread.currentThread().getContextClassLoader());
    }

    public ClasspathLSResourceResolver(ClassLoader loader) {
        this.loader = Objects.requireNonNull(loader, "loader");
    }

    @Override
    public LSInput resolveResource(
            String type,
            String namespaceURI,
            String publicId,
            String systemId,
            String baseURI
    ) {
        // We only handle classpath: URIs we authored; anything else gets ignored.
        String base = (baseURI != null && baseURI.startsWith(SCHEME)) ? baseURI.substring(SCHEME.length()) : null;
        if (base == null) return null;

        String rel = (systemId == null) ? "" : systemId;
        String baseDir = base.replaceFirst("[^/]*$", ""); // keep trailing slash
        String resourcePath = normalize(baseDir + rel);   // e.g. testschemas/nested/child.xsd

        InputStream in = loader.getResourceAsStream(resourcePath);
        if (in == null) return null; // let the parser try/complain

        return new SimpleLSInput(publicId, SCHEME + resourcePath, in);
    }

    private static String normalize(String path) {
        // collapse any "./" or "../" segments very simply; tests don’t need full RFC normalization
        // (optional; safe to return as-is if your includes are simple)
        return path.replace("/./", "/");
    }

    /** Minimal LSInput backed by a byte stream. */
    private static final class SimpleLSInput implements LSInput {
        private final String publicId;
        private final String systemId;
        private final InputStream byteStream;

        SimpleLSInput(String publicId, String systemId, InputStream byteStream) {
            this.publicId = publicId;
            this.systemId = systemId;
            this.byteStream = byteStream;
        }

        @Override public String getPublicId() { return publicId; }
        @Override public void setPublicId(String s) {}
        @Override public String getSystemId() { return systemId; }
        @Override public void setSystemId(String s) {}
        @Override public String getBaseURI() { return null; }
        @Override public void setBaseURI(String s) {}
        @Override public InputStream getByteStream() { return byteStream; }
        @Override public void setByteStream(InputStream is) {}
        @Override public java.io.Reader getCharacterStream() { return null; }
        @Override public void setCharacterStream(java.io.Reader r) {}
        @Override public String getStringData() { return null; }
        @Override public void setStringData(String s) {}
        @Override public String getEncoding() { return null; }
        @Override public void setEncoding(String s) {}
        @Override public boolean getCertifiedText() { return false; }
        @Override public void setCertifiedText(boolean b) {}
    }
}
