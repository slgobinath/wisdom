package com.javahelps.wisdom.service.exception;

import com.google.gson.JsonSyntaxException;
import spark.ExceptionHandler;
import spark.Request;
import spark.Response;

import static com.javahelps.wisdom.service.Constant.HTTP_BAD_REQUEST;
import static com.javahelps.wisdom.service.Constant.MEDIA_TEXT_PLAIN;

public class JsonSyntaxExceptionHandler implements ExceptionHandler<JsonSyntaxException> {

    @Override
    public void handle(JsonSyntaxException exception, Request request, Response response) {
        response.status(HTTP_BAD_REQUEST);
        response.type(MEDIA_TEXT_PLAIN);
        response.body("request does not have a valid JSON");
    }
}