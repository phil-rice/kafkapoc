package com.hcltech.rmg.common;


import java.util.List;
import java.util.Map;
import java.util.function.Function;

public interface Codec<From, To> {
    To encode(From from) throws Exception;

    From decode(To to) throws Exception;

    // in com.example.kafka.common.Codec
    static <T> Codec<List<T>, String> lines(Codec<T, String> itemCodec) {
        return new LineSeparatedListCodec<>(itemCodec);
    }


    static Codec<Object, String> json() {
        return new JacksonJsonCodec();
    }

    static <T> Codec<T, String> clazzCodec(Class<T> klass) {
        return new JacksonTypedJsonCodec<>(klass);
    }

    static <T> Codec<T, String> polymorphicCodec(
            Function<? super T, String> discriminatorOf,
            Map<String, Codec<? extends T, String>> subtypeCodecs
    ) {
        return new JacksonPolymorphicByCodec<>(discriminatorOf, subtypeCodecs);
    }

    static <T> Codec<T, byte[]> bytes(Codec<T, String> stringCodec) {
        return new StringToBytesCodec<>(stringCodec);
    }

}

