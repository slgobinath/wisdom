package com.javahelps.wisdom.service;

import com.javahelps.wisdom.core.WisdomApp;
import com.javahelps.wisdom.core.stream.InputHandler;
import com.javahelps.wisdom.core.stream.input.Source;
import com.javahelps.wisdom.core.stream.output.Sink;
import com.javahelps.wisdom.core.util.EventGenerator;
import com.javahelps.wisdom.service.exception.WisdomServiceException;
import com.javahelps.wisdom.service.exception.WisdomServiceExceptionMapper;
import org.wso2.msf4j.MicroservicesRunner;

import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Path("/WisdomApp")
public class WisdomService {

    private final WisdomApp wisdomApp;
    private final int wisdomPort;
    private final List<Source> sources = new ArrayList<>();
    private Map<String, InputHandler> inputHandlerMap = new HashMap<>();
    private MicroservicesRunner microservicesRunner;
    private boolean running;

    public WisdomService(WisdomApp wisdomApp, int port) {
        this.wisdomApp = wisdomApp;
        this.wisdomPort = port;
        this.microservicesRunner = new MicroservicesRunner(this.wisdomPort);
        this.microservicesRunner.addExceptionMapper(new WisdomServiceExceptionMapper());
        this.microservicesRunner.deploy(this);
    }

    public void start() {
        this.wisdomApp.start();
        this.microservicesRunner.start();
        this.sources.forEach(Source::start);
        this.running = true;
    }

    public void stop() {
        this.microservicesRunner.stop();
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

    @POST
    @Path("/{streamId}")
    @Consumes(MediaType.APPLICATION_JSON)
    public Response send(@PathParam("streamId") String streamId, Map data) {

        InputHandler inputHandler = this.inputHandlerMap.get(streamId);
        if (inputHandler != null) {
            inputHandler.send(EventGenerator.generate(data));
            return Response.accepted().build();
        } else {
            throw new WisdomServiceException(
                    String.format("The stream %s is neither defined nor not an input stream", streamId));
        }
    }

    @POST
    @Path("/admin/shutdown")
    public void shutdown() {
        this.stop();
    }

}
