package com.hcltech.rmg.common.codec;

import com.hcltech.rmg.common.errorsor.ErrorsOr;

public class MappedCodec<T, T1, To> implements Codec<T1, To> {
    private final Codec<T, To> base;
    private final Codec<T1, T> mapCodec;

    public MappedCodec(Codec<T, To> base, Codec<T1, T> mapCodec) {
        this.base = base;
        this.mapCodec = mapCodec;
    }

    @Override
    public ErrorsOr<To> encode(T1 from) {
        return mapCodec.encode(from).flatMap(base::encode);
    }

    @Override
    public ErrorsOr<T1> decode(To to) {
        return base.decode(to).flatMap(mapCodec::decode);
    }
}
