package com.javahelps.wisdom.core.exception;

/**
 * Wisdom neither propagates the exception to the user level nor crash the application instead, exceptions are passed
 * to the registered exception handlers.
 */
@FunctionalInterface
public interface ExceptionListener<T extends Exception> {
    void onException(T t);
}
