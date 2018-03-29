package com.javahelps.wisdom.manager.util;

import com.javahelps.wisdom.core.exception.WisdomAppRuntimeException;
import spark.ExceptionHandler;
import spark.Request;
import spark.Response;

import static com.javahelps.wisdom.dev.util.Constants.HTTP_BAD_REQUEST;
import static com.javahelps.wisdom.dev.util.Constants.MEDIA_TEXT_PLAIN;

public class WisdomAppRuntimeExceptionHandler implements ExceptionHandler<WisdomAppRuntimeException> {

    @Override
    public void handle(WisdomAppRuntimeException exception, Request request, Response response) {
        response.status(HTTP_BAD_REQUEST);
        response.type(MEDIA_TEXT_PLAIN);
        response.body(exception.getMessage());
    }
}
