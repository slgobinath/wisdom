package com.javahelps.wisdom.service.exception;

public class WisdomServiceException extends RuntimeException {

    public WisdomServiceException(String message) {
        super(message);
    }

    public WisdomServiceException(String message, Throwable throwable) {
        super(message, throwable);
    }

    public WisdomServiceException(Throwable throwable) {
        super(throwable);
    }
}
