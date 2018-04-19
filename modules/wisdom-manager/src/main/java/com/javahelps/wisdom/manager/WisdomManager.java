package com.javahelps.wisdom.manager;

import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;
import com.javahelps.wisdom.core.exception.WisdomAppRuntimeException;
import com.javahelps.wisdom.manager.artifact.Artifact;
import com.javahelps.wisdom.manager.artifact.ArtifactController;
import com.javahelps.wisdom.manager.exception.InvalidPropertyException;
import com.javahelps.wisdom.manager.exception.JsonSyntaxExceptionHandler;
import com.javahelps.wisdom.manager.stats.StatisticsManager;
import com.javahelps.wisdom.manager.util.InvalidPropertyExceptionHandler;
import com.javahelps.wisdom.manager.util.Utility;
import com.javahelps.wisdom.manager.util.WisdomAppRuntimeExceptionHandler;
import com.javahelps.wisdom.manager.util.WisdomParserExceptionHandler;
import com.javahelps.wisdom.query.antlr.WisdomParserException;
import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.nodes.Tag;
import org.yaml.snakeyaml.representer.Representer;
import spark.Spark;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;

import static com.javahelps.wisdom.dev.util.Constants.MEDIA_TEXT_PLAIN;
import static com.javahelps.wisdom.manager.util.Constants.*;

public class WisdomManager {

    private static final Logger LOGGER = LoggerFactory.getLogger(WisdomManager.class);

    private final Yaml yaml;
    private final int managerPort;
    private final ArtifactController controller;
    private final StatisticsManager statisticsManager;
    private final Gson gson = new Gson();
    private final Path wisdomHomePath;


    public WisdomManager(int managerPort) throws IOException {
        this(System.getenv(WISDOM_HOME), managerPort);
    }

    public WisdomManager(String wisdomHome, int managerPort) throws IOException {

        if (wisdomHome == null) {
            throw new WisdomAppRuntimeException("%s environment variable not set", WISDOM_HOME);
        }

        this.wisdomHomePath = Paths.get(wisdomHome);
        Path configDirPath = this.wisdomHomePath.resolve(CONF_DIR);
        if (!Files.exists(configDirPath)) {
            throw new WisdomAppRuntimeException("%s does not exist", configDirPath);
        }

        Representer representer = new Representer();
        representer.addClassTag(Artifact.class, Tag.MAP);
        this.yaml = new Yaml(representer);

        // Initialize from configuration
        Map<String, Object> configuration = Utility.readYaml(this.yaml, configDirPath.resolve("config.yaml"), false);

        this.managerPort = managerPort;
        this.controller = new ArtifactController(this.wisdomHomePath, configDirPath.resolve(ARTIFACTS_CONFIG_FILE), configuration, yaml);
        this.statisticsManager = new StatisticsManager("WisdomManager_" + managerPort, this.controller, configuration);
    }

    public void start() {
        LOGGER.info("Starting Wisdom Manager at {}", this.managerPort);
        Spark.port(this.managerPort);
        Spark.exception(InvalidPropertyException.class, new InvalidPropertyExceptionHandler());
        Spark.exception(NullPointerException.class, new InvalidPropertyExceptionHandler());
        Spark.exception(WisdomParserException.class, new WisdomParserExceptionHandler());
        Spark.exception(WisdomAppRuntimeException.class, new WisdomAppRuntimeExceptionHandler());
        Spark.exception(JsonSyntaxException.class, new JsonSyntaxExceptionHandler());
        // Deploy new artifact
        Spark.post("/WisdomManager/app", (request, response) -> {
            Map<String, Object> deployer = this.gson.fromJson(request.body(), Map.class);
            Utility.validateProperties(deployer, new String[]{"query", "port"}, new Class[]{String.class, Number.class});
            String query = (String) deployer.get("query");
            int port = ((Number) deployer.get("port")).intValue();
            this.controller.deploy(query, port);
            response.type(MEDIA_TEXT_PLAIN);
            response.status(HTTP_CREATED);
            return "Deployed Wisdom app successfully";
        });
        // Update initializable variables
        Spark.patch("/WisdomManager/app/:appName", (request, response) -> {
            String appName = request.params("appName");
            Map<String, Map<String, Comparable>> map = this.gson.fromJson(request.body(), Map.class);
            return this.controller.initialize(appName, map);
        });
        // Get deployed artifact information
        Spark.get("/WisdomManager/app/:appName", (request, response) -> this.controller.info(request.params("appName")), this.gson::toJson);
        // Get information of all deployed artifacts
        Spark.get("/WisdomManager/app", (request, response) -> this.controller.info(), this.gson::toJson);
        // Delete an artifact
        Spark.delete("/WisdomManager/app/:appName", (request, response) -> this.controller.delete(request.params("appName")));
        // Start an artifact
        Spark.post("/WisdomManager/start/:appName", (request, response) -> this.controller.start(request.params("appName")));
        // Stop an artifact
        Spark.post("/WisdomManager/stop/:appName", (request, response) -> this.controller.stop(request.params("appName")));
        // Stop the manager
        Spark.post("/WisdomManager/stop", ((request, response) -> this.shutdown()));
        this.controller.start();
        this.statisticsManager.start();
    }

    public String shutdown() {
        this.statisticsManager.stop();
        this.controller.shutdown();
        new Thread(() -> {
            try {
                Thread.sleep(100L);
            } catch (InterruptedException e) {
                // Do nothing
            }
            try {
                Spark.stop();
            } finally {
                System.exit(0);
            }
        }).start();
        return "Shutting down wisdom manager...";
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
                .help("port number for Wisdom manager");
        try {
            // Parse arguments
            final Namespace namespace = parser.parseArgs(args);
            int managerPort = namespace.getInt("port");

            WisdomManager manager;
            try {
                manager = new WisdomManager(managerPort);
            } catch (IOException e) {
                e.printStackTrace(System.err);
                return;
            }
            manager.start();
        } catch (ArgumentParserException e) {
            e.printStackTrace(System.err);
        }
    }
}
