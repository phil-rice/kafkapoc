package com.hcltech.rmg.oneliner;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

public class OneLineIt {
    public static void main(String[] args) throws IOException {
        try (var in = OneLineIt.class.getResourceAsStream("/cel.cel")) {
            if (in == null) {
                throw new IllegalStateException("Resource not found: cel.cel (is it in the same package?)");
            }
            String asString = new String(in.readAllBytes(), StandardCharsets.UTF_8);
            System.out.println(asString);
        }
    }
}
