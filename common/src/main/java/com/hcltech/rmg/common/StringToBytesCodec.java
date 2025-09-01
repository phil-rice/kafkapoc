package com.hcltech.rmg.common;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Objects;

public final class StringToBytesCodec<T> implements Codec<T, byte[]> {
    private final Codec<T, String> delegate;
    private final Charset charset;

    public StringToBytesCodec(Codec<T, String> delegate) {
        this(delegate, StandardCharsets.UTF_8);
    }

    public StringToBytesCodec(Codec<T, String> delegate, Charset charset) {
        this.delegate = Objects.requireNonNull(delegate, "delegate");
        this.charset = Objects.requireNonNull(charset, "charset");
    }

    @Override
    public byte[] encode(T from) throws Exception {
        String s = delegate.encode(from);
        return s.getBytes(charset);
    }

    @Override
    public T decode(byte[] to) throws Exception {
        String s = new String(to, charset);
        return delegate.decode(s);
    }
}
