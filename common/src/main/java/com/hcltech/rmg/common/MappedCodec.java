package com.hcltech.rmg.common;

import java.util.Map;
import java.util.function.Function;

public class MappedCodec<T, T1, To> implements Codec<T1, To> {
    private final Codec<T, To> base;
    private final Codec<T1, T> mapCodec;

    public MappedCodec(Codec<T, To> base, Codec<T1, T> mapCodec) {
        this.base = base;
        this.mapCodec = mapCodec;
    }

    @Override
    public To encode(T1 from) throws Exception {
        T encode = mapCodec.encode(from);
        return base.encode(encode);
    }

    @Override
    public T1 decode(To to) throws Exception {
        T decode = base.decode(to);
        return mapCodec.decode(decode);
    }
}
