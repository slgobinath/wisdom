package com.javahelps.wisdom.service;

import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;
import com.javahelps.wisdom.core.WisdomApp;
import com.javahelps.wisdom.core.stream.InputHandler;
import com.javahelps.wisdom.core.stream.input.Source;
import com.javahelps.wisdom.core.stream.output.Sink;
import com.javahelps.wisdom.core.util.EventGenerator;
import com.javahelps.wisdom.service.exception.JsonSyntaxExceptionHandler;
import com.javahelps.wisdom.service.exception.WisdomServiceException;
import com.javahelps.wisdom.service.exception.WisdomServiceExceptionHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import spark.Request;
import spark.Response;
import spark.Spark;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.javahelps.wisdom.service.Constant.HTTP_ACCEPTED;
import static com.javahelps.wisdom.service.Constant.MEDIA_APPLICATION_JSON;

public class WisdomService {

    private static final Logger LOGGER = LoggerFactory.getLogger(WisdomService.class);

    private final WisdomApp wisdomApp;
    private final int wisdomPort;
    private final List<Source> sources = new ArrayList<>();
    private Map<String, InputHandler> inputHandlerMap = new HashMap<>();
    private boolean running;
    private final Gson gson = new Gson();

    public WisdomService(WisdomApp wisdomApp, int port) {
        this.wisdomApp = wisdomApp;
        this.wisdomPort = port;
    }

    public void start() {
        this.wisdomApp.start();
        this.sources.forEach(Source::start);
        this.running = true;
        Spark.port(this.wisdomPort);
        Spark.exception(WisdomServiceException.class, new WisdomServiceExceptionHandler());
        Spark.exception(JsonSyntaxException.class, new JsonSyntaxExceptionHandler());
        Spark.post("/WisdomApp/:streamId", MEDIA_APPLICATION_JSON, this::send);
        Spark.post("/WisdomApp/admin/shutdown", (request, response) -> {
            this.stop();
            return null;
        });
    }

    public void stop() {
        LOGGER.debug("Shutting down Wisdom server");
        Spark.stop();
        this.sources.forEach(Source::stop);
        this.wisdomApp.shutdown();
        this.running = false;
    }

    public boolean isRunning() {
        return this.running;
    }

    public void addSink(String streamId, Sink sink) {
        this.wisdomApp.addSink(streamId, sink);
    }

    public void addSource(String streamId) {
        InputHandler inputHandler = this.wisdomApp.getInputHandler(streamId);
        this.inputHandlerMap.put(streamId, inputHandler);
    }

    public void addSource(String streamId, Source source) {
        InputHandler inputHandler = this.wisdomApp.getInputHandler(streamId);
        source.init(this.wisdomApp, streamId, inputHandler);
        this.sources.add(source);
    }

    private Response send(Request request, Response response) {

        String streamId = request.params("streamId");
        Map<String, Comparable> data = this.gson.fromJson(request.body(), Map.class);
        LOGGER.debug("Received event for {}:{}", streamId, data);
        InputHandler inputHandler = this.inputHandlerMap.get(streamId);
        if (inputHandler != null) {
            inputHandler.send(EventGenerator.generate(data));
            response.status(HTTP_ACCEPTED);
        } else {
            throw new WisdomServiceException(
                    String.format("The stream %s is neither defined nor not an input stream", streamId));
        }
        return response;
    }

}
