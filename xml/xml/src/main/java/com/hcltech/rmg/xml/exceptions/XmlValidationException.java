package com.hcltech.rmg.xml.exceptions;

public final class XmlValidationException extends RuntimeException {
    public XmlValidationException(String message, Throwable cause) { super(message, cause); }
    public XmlValidationException(String message) { super(message); }
}