package com.javahelps.wisdom.service;

import com.google.gson.JsonSyntaxException;
import com.javahelps.wisdom.core.WisdomApp;
import com.javahelps.wisdom.core.extension.ImportsManager;
import com.javahelps.wisdom.core.stream.InputHandler;
import com.javahelps.wisdom.core.stream.input.Source;
import com.javahelps.wisdom.core.util.Commons;
import com.javahelps.wisdom.query.WisdomCompiler;
import com.javahelps.wisdom.service.exception.JsonSyntaxExceptionHandler;
import com.javahelps.wisdom.service.exception.WisdomServiceException;
import com.javahelps.wisdom.service.exception.WisdomServiceExceptionHandler;
import com.javahelps.wisdom.service.sink.HTTPSink;
import com.javahelps.wisdom.service.sink.KafkaSink;
import com.javahelps.wisdom.service.source.HTTPSource;
import com.javahelps.wisdom.service.source.KafkaSource;
import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import spark.Spark;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;

import static com.javahelps.wisdom.core.util.WisdomConstants.THRESHOLD_STREAM;
import static com.javahelps.wisdom.service.Constant.JSON;
import static com.javahelps.wisdom.service.Constant.MAPPING;

public class WisdomService {

    static {
        // Supported sources
        ImportsManager.INSTANCE.use(HTTPSource.class);
        ImportsManager.INSTANCE.use(KafkaSource.class);
        // Supported sinks
        ImportsManager.INSTANCE.use(HTTPSink.class);
        ImportsManager.INSTANCE.use(KafkaSink.class);
    }

    private static final Logger LOGGER = LoggerFactory.getLogger(WisdomService.class);

    private final WisdomApp wisdomApp;
    private final int wisdomPort;
    private Map<String, InputHandler> inputHandlerMap = new HashMap<>();
    private boolean running;


    public WisdomService(WisdomApp wisdomApp, int port) {
        this.wisdomApp = wisdomApp;
        this.wisdomPort = port;
        // Always add HTTP source for _ThresholdStream
        if (wisdomApp.getStream(THRESHOLD_STREAM) != null) {
            wisdomApp.addSource(THRESHOLD_STREAM, Source.create("http", Commons.map(MAPPING, JSON)));
        }
    }

    public void start() {
        this.running = true;
        Spark.port(this.wisdomPort);
        Spark.exception(WisdomServiceException.class, new WisdomServiceExceptionHandler());
        Spark.exception(JsonSyntaxException.class, new JsonSyntaxExceptionHandler());
        Spark.post("/WisdomApp/admin/shutdown", (request, response) -> {
            this.stop();
            return null;
        });
        this.wisdomApp.start();
    }

    public void stop() {
        LOGGER.debug("Shutting down Wisdom server");
        Spark.stop();
        this.wisdomApp.shutdown();
        this.running = false;
    }

    public boolean isRunning() {
        return this.running;
    }

    public static void main(String[] args) {
        // Define arguments
        ArgumentParser parser = ArgumentParsers.newFor("wisdom-service")
                .cjkWidthHack(true)
                .noDestConversionForPositionalArgs(true)
                .singleMetavar(true)
                .terminalWidthDetection(true)
                .build();
        parser.addArgument("--port")
                .required(false)
                .setDefault(8080)
                .type(Integer.class)
                .dest("port")
                .help("port number for Wisdom service");
        parser.addArgument("file")
                .required(true)
                .type(String.class)
                .dest("queryFile")
                .help("wisdom query script to load");
        try {
            // Parse arguments
            final Namespace response = parser.parseArgs(args);
            int port = response.getInt("port");
            Path queryPath = Paths.get(response.getString("queryFile"));

            if (!Files.exists(queryPath)) {
                System.err.print("Wisdom query file " + queryPath + " not found");
            }

            // Start wisdom service
            try {
                WisdomApp app = WisdomCompiler.parse(queryPath);
                WisdomService service = new WisdomService(app, port);
                service.start();
            } catch (Exception e) {
                e.printStackTrace(System.err);
            }

        } catch (ArgumentParserException e) {
            e.printStackTrace(System.err);
        }
    }
}
