package com.hcltech.rmg.common;


import java.util.List;
import java.util.Map;
import java.util.function.Function;

public interface Codec<From, To> {

    To encode(From from) throws Exception;

    From decode(To to) throws Exception;

    default Codec<To, From> invert() {
        return new Codec<To, From>() {
            @Override
            public From encode(To p) throws Exception {
                return Codec.this.decode(p);
            }

            @Override
            public To decode(From from) throws Exception {
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

    // Example We want a Codec<ValueEnvelope<T>, Map<String,Object>
    //We have Codec<T, Map<String,Object>> base
    //We need to know how to Codec<ValueEnvelop<Map<String,Object>>, ValueEnvelope<T>>
    //It is just impossible to do this cleanly in Java. Type system sucks. We can't even represent it...
    static <HolderT, HolderT1, T, T1, To> Codec<HolderT1, To> mapped(Codec<HolderT, HolderT1> base, Codec<T, To> mapCodec) {
        return new MappedCodec<T, T1, To>(base, mapCodec);

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

