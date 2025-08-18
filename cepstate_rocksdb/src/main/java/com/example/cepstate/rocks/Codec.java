package com.example.cepstate.rocks;


public interface Codec<From, To> {
    To encode(From from) throws Exception;

    From decode(To to) throws Exception;
}
