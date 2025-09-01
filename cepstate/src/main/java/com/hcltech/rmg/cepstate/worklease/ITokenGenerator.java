package com.hcltech.rmg.cepstate.worklease;

public interface ITokenGenerator {
    String next(String domainId, long offset);

    static  ITokenGenerator generator() {
        return (domainId, offset) -> domainId + "-" + offset;
    }
}
