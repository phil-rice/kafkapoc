package com.hcltech.rmg.common.codec;

import com.hcltech.rmg.common.errorsor.ErrorsOr;

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
    public ErrorsOr<byte[]> encode(T from) {
        try {
            return delegate.encode(from).map(x -> x.getBytes(charset));
        } catch (Exception e) {
            return ErrorsOr.error("Failed to encode to bytes: " + e.getMessage());
        }
    }

    @Override
    public ErrorsOr<T> decode(byte[] to) {
        String s = new String(to, charset);
        return delegate.decode(s);
    }
}
