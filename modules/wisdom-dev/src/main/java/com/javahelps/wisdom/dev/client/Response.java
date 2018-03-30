package com.javahelps.wisdom.dev.client;

public class Response {
    private int status;
    private String reason;

    public Response(int status, String reason) {
        this.status = status;
        this.reason = reason;
    }

    public int getStatus() {
        return status;
    }

    public String getReason() {
        return reason;
    }
}