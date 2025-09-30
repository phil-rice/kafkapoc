package com.hcltech.rmg.xml.exceptions;

public final class LoadSchemaException extends RuntimeException {
    public LoadSchemaException(String message, Throwable cause) { super(message, cause); }
    public LoadSchemaException(String message) { super(message); }
}