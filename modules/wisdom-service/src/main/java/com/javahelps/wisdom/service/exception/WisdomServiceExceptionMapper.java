package com.javahelps.wisdom.service.exception;

import javax.ws.rs.core.Response;
import javax.ws.rs.ext.ExceptionMapper;

public class WisdomServiceExceptionMapper implements ExceptionMapper<WisdomServiceException> {

    @Override
    public Response toResponse(WisdomServiceException ex) {
        return Response.status(400).
                entity(ex.getMessage()).
                type("text/plain").
                build();
    }
}