package com.javahelps.wisdom.core.exception;

/**
 * Exception during {@link com.javahelps.wisdom.core.WisdomApp} execution.
 */
public class AttributeNotFoundException extends WisdomAppRuntimeException {

    public AttributeNotFoundException(String message) {
        super(message);
    }

    public AttributeNotFoundException(String message, Throwable throwable) {
        super(message, throwable);
    }

    public AttributeNotFoundException(Throwable throwable) {
        super(throwable);
    }
}
