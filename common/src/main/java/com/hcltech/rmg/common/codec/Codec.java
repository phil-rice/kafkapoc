package com.hcltech.rmg.common.codec;


import com.hcltech.rmg.common.errorsor.ErrorsOr;

import java.util.List;
import java.util.Map;
import java.util.function.Function;

public interface Codec<From, To> {

    ErrorsOr<To> encode(From from) ;

    ErrorsOr<From> decode(To to) ;

    default Codec<To, From> invert() {
        return new Codec<To, From>() {
            @Override
            public ErrorsOr<From> encode(To p) {
                return Codec.this.decode(p);
            }

            @Override
            public ErrorsOr<To> decode(From from) {
                return Codec.this.encode(from);
            }
        };
    }

    // in com.example.kafka.common.Codec
    static <T> Codec<List<T>, String> lines(Codec<T, String> itemCodec) {
        return new LineSeparatedListCodec<>(itemCodec);
    }

    static <T> Codec<T, Map<String, Object>> jsonTree(Class<T> klass) {
        return new JacksonTreeCodec<>(klass);  // implement with Jackson’s ObjectMapper.convertValue
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

