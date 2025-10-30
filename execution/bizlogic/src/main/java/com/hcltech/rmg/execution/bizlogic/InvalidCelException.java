package com.hcltech.rmg.execution.bizlogic;

import java.util.ArrayList;

public class InvalidCelException extends RuntimeException {
    public InvalidCelException(String s, ArrayList<Object> allCelErrors) {
        super(s + " : " + allCelErrors.toString());
    }
}
