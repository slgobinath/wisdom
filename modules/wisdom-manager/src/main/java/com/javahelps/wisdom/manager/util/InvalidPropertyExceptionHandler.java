package com.javahelps.wisdom.manager.util;

import spark.ExceptionHandler;
import spark.Request;
import spark.Response;

import static com.javahelps.wisdom.dev.util.Constants.HTTP_BAD_REQUEST;
import static com.javahelps.wisdom.dev.util.Constants.MEDIA_TEXT_PLAIN;

public class InvalidPropertyExceptionHandler implements ExceptionHandler<RuntimeException> {

    @Override
    public void handle(RuntimeException exception, Request request, Response response) {
        response.status(HTTP_BAD_REQUEST);
        response.type(MEDIA_TEXT_PLAIN);
        response.body(exception.getMessage());
    }
}
