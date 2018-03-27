package com.javahelps.wisdom.manager.exception;

public class InvalidPropertyException extends RuntimeException {

    public InvalidPropertyException(String message) {
        super(message);
    }

    public InvalidPropertyException(String message, Object... args) {
        this(String.format(message, args));
    }

    public InvalidPropertyException(Throwable throwable, String message, Object... args) {
        this(String.format(message, args), throwable);
    }

    public InvalidPropertyException(String message, Throwable throwable) {
        super(message, throwable);
    }

    public InvalidPropertyException(Throwable throwable) {
        super(throwable);
    }
}
