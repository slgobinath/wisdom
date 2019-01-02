/*
 * Copyright (c) 2018, Gobinath Loganathan (http://github.com/slgobinath) All Rights Reserved.
 *
 * Gobinath licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. In addition, if you are using
 * this file in your research work, you are required to cite
 * WISDOM as mentioned at https://github.com/slgobinath/wisdom.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.javahelps.wisdom.service;

import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;
import com.javahelps.wisdom.core.WisdomApp;
import com.javahelps.wisdom.core.extension.ImportsManager;
import com.javahelps.wisdom.core.stream.input.Source;
import com.javahelps.wisdom.core.util.Commons;
import com.javahelps.wisdom.query.WisdomCompiler;
import com.javahelps.wisdom.service.exception.JsonSyntaxExceptionHandler;
import com.javahelps.wisdom.service.exception.WisdomServiceException;
import com.javahelps.wisdom.service.exception.WisdomServiceExceptionHandler;
import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import spark.Spark;

import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;

import static com.javahelps.wisdom.core.util.WisdomConstants.THRESHOLD_STREAM;
import static com.javahelps.wisdom.dev.util.Constants.JSON;
import static com.javahelps.wisdom.dev.util.Constants.MAPPING;

public class WisdomService {

    private static final Logger LOGGER = LoggerFactory.getLogger(WisdomService.class);

    static {
        ImportsManager.INSTANCE.scanClassPath();
    }

    private final int wisdomPort;
    private final WisdomApp wisdomApp;
    private final Gson gson = new Gson();
    private boolean running;


    public WisdomService(WisdomApp wisdomApp, int port) {
        this.wisdomApp = wisdomApp;
        this.wisdomPort = port;
        // Always add HTTP source for _ThresholdStream
        if (wisdomApp.getStream(THRESHOLD_STREAM) != null) {
            wisdomApp.addSource(THRESHOLD_STREAM, Source.create("http", Commons.map(MAPPING, JSON)));
        }
        wisdomApp.getProperties().put("port", port);
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
                .setDefault(8888)
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

    public void start() {
        this.running = true;
        Spark.port(this.wisdomPort);
        Spark.exception(WisdomServiceException.class, new WisdomServiceExceptionHandler());
        Spark.exception(JsonSyntaxException.class, new JsonSyntaxExceptionHandler());
        Spark.post("/WisdomApp/admin/shutdown", (request, response) -> {
            new Thread(() -> {
                try {
                    Thread.sleep(50L);
                } catch (InterruptedException e) {
                    // Do nothing
                }
                try {
                    this.stop();
                } finally {
                    // System.exit(0);
                }
            }).start();
            return "Shutting down wisdom service...";
        });
        Spark.get("/WisdomApp/admin/info", (request, response) -> this.info(), gson::toJson);
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

    public Map<String, Object> info() {
        RuntimeMXBean rb = ManagementFactory.getRuntimeMXBean();
        return Commons.map("running", this.running,
                "name", this.wisdomApp.getName(),
                "version", this.wisdomApp.getVersion(),
                "port", this.wisdomPort,
                "uptime", rb.getUptime());
    }
}
