package com.javahelps.wisdom.service.exception;

import spark.ExceptionHandler;
import spark.Request;
import spark.Response;

import static com.javahelps.wisdom.dev.util.Constants.HTTP_BAD_REQUEST;
import static com.javahelps.wisdom.dev.util.Constants.MEDIA_TEXT_PLAIN;

public class WisdomServiceExceptionHandler implements ExceptionHandler<WisdomServiceException> {

    @Override
    public void handle(WisdomServiceException ex, Request request, Response response) {
        response.status(HTTP_BAD_REQUEST);
        response.type(MEDIA_TEXT_PLAIN);
        response.body(ex.getMessage());
    }
}