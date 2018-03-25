package com.javahelps.wisdom.core.exception;

/**
 * Exception during {@link com.javahelps.wisdom.core.WisdomApp} execution.
 */
public class WisdomAppRuntimeException extends RuntimeException {

    public WisdomAppRuntimeException(String message) {
        super(message);
    }

    public WisdomAppRuntimeException(String message, Throwable throwable) {
        super(message, throwable);
    }

    public WisdomAppRuntimeException(String message, Object... args) {
        this(String.format(message, args));
    }

    public WisdomAppRuntimeException(Throwable throwable) {
        super(throwable);
    }
}
