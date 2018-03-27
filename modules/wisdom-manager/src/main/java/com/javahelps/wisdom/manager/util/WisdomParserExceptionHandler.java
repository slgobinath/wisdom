package com.javahelps.wisdom.manager.util;

import com.javahelps.wisdom.manager.exception.InvalidPropertyException;
import com.javahelps.wisdom.query.antlr.WisdomParserException;
import spark.ExceptionHandler;
import spark.Request;
import spark.Response;

import static com.javahelps.wisdom.service.Constant.HTTP_BAD_REQUEST;
import static com.javahelps.wisdom.service.Constant.MEDIA_TEXT_PLAIN;

public class WisdomParserExceptionHandler implements ExceptionHandler<WisdomParserException> {

    @Override
    public void handle(WisdomParserException exception, Request request, Response response) {
        response.status(HTTP_BAD_REQUEST);
        response.type(MEDIA_TEXT_PLAIN);
        response.body(exception.getMessage());
    }
}
