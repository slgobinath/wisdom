package com.javahelps.wisdom.service.util;

import com.google.gson.JsonObject;
import org.wso2.msf4j.MicroservicesRunner;

import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.core.MediaType;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;

@Path("/streamReceiver")
public class TestServer {

    private static final String OUTPUT_FILE = "test_server_output.log";
    private static final int PORT = 9999;
    private static long waitingTime = 10_000L;
    private MicroservicesRunner microservicesRunner;

    private static void appendToFile(String data) {

        try {
            Files.write(Paths.get(OUTPUT_FILE), Arrays.asList(data), StandardOpenOption.APPEND);
        } catch (IOException e) {
        }
    }

    public static void main(String[] args) {
        if (args.length > 0) {
            try {
                long value = Long.parseLong(args[0]);
                waitingTime = value;
            } catch (NumberFormatException e) {
            }
        }
        new TestServer().start();
    }

    @POST
    @Path("/")
    @Consumes(MediaType.APPLICATION_JSON)
    public void receive(JsonObject msg) {

        new Thread(() -> appendToFile(msg.toString())).start();
    }

    private synchronized void start() {

        appendToFile("INFO: Starting up the test server");
        this.microservicesRunner = new MicroservicesRunner(PORT);
        this.microservicesRunner.deploy(this);
        this.microservicesRunner.start();

        sleep(waitingTime);
        this.stop();
    }

    private synchronized void stop() {

        appendToFile("INFO: Shutting down the test server");
        try {
            this.microservicesRunner.stop();
        } catch (IllegalStateException ex) {
        }
        sleep(100);
        System.exit(0);
    }

    private void sleep(long timestamp) {
        try {
            Thread.sleep(timestamp);
        } catch (InterruptedException e) {
        }
    }
}
