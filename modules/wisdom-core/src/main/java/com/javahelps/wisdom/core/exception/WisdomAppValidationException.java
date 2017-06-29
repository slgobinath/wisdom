package com.javahelps.wisdom.core.exception;

/**
 * Exception due to invalid {@link com.javahelps.wisdom.core.WisdomApp}.
 */
public class WisdomAppValidationException extends RuntimeException {

    public WisdomAppValidationException(String message) {
        super(message);
    }

    public WisdomAppValidationException(String message, Throwable throwable) {
        super(message, throwable);
    }

    public WisdomAppValidationException(Throwable throwable) {
        super(throwable);
    }
}
