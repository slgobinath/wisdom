package com.javahelps.wisdom.core.exception;

/**
 * Exception due to invalid {@link com.javahelps.wisdom.core.event.Event}.
 */
public class EventValidationException extends RuntimeException {

    public EventValidationException(String message) {
        super(message);
    }
}
