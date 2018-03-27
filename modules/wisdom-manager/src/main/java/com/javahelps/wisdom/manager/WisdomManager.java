package com.javahelps.wisdom.manager;

import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;
import com.javahelps.wisdom.core.WisdomApp;
import com.javahelps.wisdom.core.exception.WisdomAppRuntimeException;
import com.javahelps.wisdom.core.extension.ImportsManager;
import com.javahelps.wisdom.core.variable.Variable;
import com.javahelps.wisdom.manager.entity.Artifact;
import com.javahelps.wisdom.manager.exception.InvalidPropertyException;
import com.javahelps.wisdom.manager.util.InvalidPropertyExceptionHandler;
import com.javahelps.wisdom.manager.util.Utility;
import com.javahelps.wisdom.manager.util.WisdomAppRuntimeExceptionHandler;
import com.javahelps.wisdom.manager.util.WisdomParserExceptionHandler;
import com.javahelps.wisdom.query.WisdomCompiler;
import com.javahelps.wisdom.query.antlr.WisdomParserException;
import com.javahelps.wisdom.service.client.WisdomAdminClient;
import com.javahelps.wisdom.service.exception.JsonSyntaxExceptionHandler;
import com.javahelps.wisdom.service.exception.WisdomServiceException;
import com.javahelps.wisdom.service.exception.WisdomServiceExceptionHandler;
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

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

import static com.javahelps.wisdom.core.util.Commons.map;
import static com.javahelps.wisdom.core.util.WisdomConstants.PRIORITY;
import static com.javahelps.wisdom.core.util.WisdomConstants.THRESHOLD_STREAM;
import static com.javahelps.wisdom.manager.util.Constants.*;
import static com.javahelps.wisdom.service.Constant.MEDIA_TEXT_PLAIN;

public class WisdomManager {

    static {
        ImportsManager.INSTANCE.scanClassPath();
    }

    private static final Logger LOGGER = LoggerFactory.getLogger(WisdomManager.class);

    private final Yaml yaml;
    private final Path artifactsPath;
    private final Path configDirPath;
    private final Path artifactsConfigPath;
    private final Map<String, Artifact> deployedArtifacts = new HashMap<>();
    private final Gson gson = new Gson();
    private final Path wisdomHomePath;
    private final int minServicePort;
    private final int maxServicePort;
    private final String JAVA_BIN = Paths.get(System.getProperty("java.home"), "bin/java").toAbsolutePath().toString();

    public WisdomManager() throws IOException {
        this(System.getenv(WISDOM_HOME));
    }

    public WisdomManager(String wisdomHome) throws IOException {

        if (wisdomHome == null) {
            throw new WisdomAppRuntimeException("%s environment variable not set", WISDOM_HOME);
        }
        this.wisdomHomePath = Paths.get(wisdomHome);

        Representer representer = new Representer();
        representer.addClassTag(Artifact.class, Tag.MAP);
        this.yaml = new Yaml(representer);
        this.artifactsPath = wisdomHomePath.resolve(ARTIFACTS_DIR);
        this.configDirPath = wisdomHomePath.resolve(CONF_DIR);
        this.artifactsConfigPath = this.configDirPath.resolve(ARTIFACTS_CONFIG_FILE);
        if (!Files.exists(this.artifactsPath)) {
            Files.createDirectories(this.artifactsPath);
        }
        if (!Files.exists(this.configDirPath)) {
            Files.createDirectories(this.configDirPath);
        }
        // Initialize from configuration
        Map<String, Object> configuration = Utility.readYaml(this.yaml, this.configDirPath.resolve("config.yaml"), false);
        Map<String, Object> wisdomService = (Map<String, Object>) configuration.get("wisdom_service");
        if (wisdomService == null) {
            wisdomService = Collections.emptyMap();
        }
        this.minServicePort = (int) wisdomService.getOrDefault("min_port", 8080);
        this.maxServicePort = (int) wisdomService.getOrDefault("max_port", 8888);
        this.loadArtifactsConfig();
    }

    public void deploy(String query, int port) throws IOException {

        Objects.requireNonNull(query, "query is not provided");
        // Port must be within range
        if (port < this.minServicePort || port > this.maxServicePort) {
            throw new WisdomAppRuntimeException("Wisdom service port must be within %d - %d but found %d", this.minServicePort, this.maxServicePort, port);
        }

        WisdomApp app = WisdomCompiler.parse(query);
        String fileName = app.getName() + ".wisdomql";

        // Port should not be assigned to existing app
        for (Artifact artifact : this.deployedArtifacts.values()) {
            if (port == artifact.getPort() && !fileName.equals(artifact.getFile())) {
                throw new WisdomAppRuntimeException("Port %d is already assigned to another Wisdom app: %s", port, artifact.getFile().replace(".wisdomql", ""));
            }
        }

        Artifact artifact = new Artifact();
        artifact.setFile(fileName);
        artifact.setPort(port);
        artifact.setPriority(((Long) app.getProperties().getOrDefault(PRIORITY, 10L)).intValue());

        List<Variable> trainableVariables = app.getTrainable();
        for (Variable variable : trainableVariables) {
            artifact.addInit(THRESHOLD_STREAM, variable.getId(), (Comparable) variable.get());
        }
        deployedArtifacts.put(app.getName(), artifact);
        Path target = this.artifactsPath.resolve(fileName);
        try {
            Files.write(target, query.getBytes());
            this.saveArtifactsConfig();
        } catch (IOException e) {
            Files.deleteIfExists(target);
            throw new WisdomAppRuntimeException("Failed to deploy " + app.getName(), e);
        }
    }

    public String start(String appName) {

        Objects.requireNonNull(appName, "Wisdom appName is not provided");
        Artifact artifact = deployedArtifacts.get(appName);
        if (artifact == null) {
            throw new WisdomAppRuntimeException("Wisdom app: '%s' not found in deployed applications", appName);
        }

        // Test if it is already running
        try (WisdomAdminClient client = new WisdomAdminClient(artifact.getHost(), artifact.getPort())) {
            Map<String, Comparable> info = client.info();
            if (info != null) {
                if (info != null) {
                    return String.format("Wisdom app '%s' is already running on %d for %.2f seconds", appName, info.get("port"), ((Number) info.get("uptime")).doubleValue() / 1000);
                }
            }
        } catch (IOException e) {
            // Not running
        }

        String[] command = {
                JAVA_BIN,
                "-classpath",
                System.getProperty("java.class.path"),
                "com.javahelps.wisdom.service.WisdomService",
                "--port",
                Integer.toString(artifact.getPort()),
                this.artifactsPath.resolve(artifact.getFile()).toAbsolutePath().toString()
        };
        ProcessBuilder builder = new ProcessBuilder(command);
        builder.directory(this.wisdomHomePath.toFile());
        Process process;
        try {
            process = builder.start();
        } catch (IOException e) {
            LOGGER.error("Failed to start wisdom service for " + appName, e);
            throw new WisdomAppRuntimeException("Failed to start wisdom service");
        }
        artifact.setPid(process.pid());
        this.saveArtifactsConfig();
        return "Application started successfully";
    }

    public String stop(String appName) {

        Objects.requireNonNull(appName, "Wisdom appName is not provided");
        Artifact artifact = deployedArtifacts.get(appName);
        if (artifact == null) {
            throw new WisdomAppRuntimeException("Wisdom app: '%s' not found in deployed applications", appName);
        }

        // Test if it is already running
        try (WisdomAdminClient client = new WisdomAdminClient(artifact.getHost(), artifact.getPort())) {
            Map<String, Comparable> info = client.info();
            if (info == null) {
                return String.format("Wisdom app %s is not running", appName);
            } else {
                client.stop();
            }
        } catch (IOException e) {
            // Not running
        }
        artifact.setPid(-1L);
        this.saveArtifactsConfig();
        return String.format("Wisdom app %s is successfully stopped", appName);
    }

    public String delete(String appName) {

        this.stop(appName);
        Artifact artifact = deployedArtifacts.remove(appName);
        try {
            Files.deleteIfExists(this.artifactsPath.resolve(artifact.getFile()));
            this.saveArtifactsConfig();
        } catch (IOException e) {
            LOGGER.error("Error in deleting " + artifact.getFile(), e);
            throw new WisdomAppRuntimeException("Error in deleting " + artifact.getFile());
        }
        return String.format("Wisdom app %s is successfully deleted", appName);
    }

    public Map<String, Comparable> info(String appName) {

        Objects.requireNonNull(appName, "Wisdom appName is not provided");
        Artifact artifact = deployedArtifacts.get(appName);
        if (artifact == null) {
            throw new WisdomAppRuntimeException("Wisdom app: '%s' not found in deployed applications", appName);
        }

        Map<String, Comparable> info = map("name", appName, "port", artifact.getPort(), "pid", artifact.getPid(), "running", false);

        // Test if it is running
        try (WisdomAdminClient client = new WisdomAdminClient(artifact.getHost(), artifact.getPort())) {
            Map<String, Comparable> serviceInfo = client.info();
            if (serviceInfo != null) {
                info.put("running", serviceInfo.get("running"));
            }
        } catch (IOException e) {
            // Not running
        }
        return info;
    }

    private void loadArtifactsConfig() {
        this.deployedArtifacts.clear();
        Map<String, Object> config = Utility.readYaml(this.yaml, this.artifactsConfigPath, true);
        for (Map.Entry<String, Object> entry : config.entrySet()) {
            this.deployedArtifacts.put(entry.getKey(), new Artifact((Map<String, Object>) entry.getValue()));
        }
    }

    private void saveArtifactsConfig() {
        try (BufferedWriter writer = Files.newBufferedWriter(this.artifactsConfigPath)) {
            this.yaml.dump(deployedArtifacts, writer);
        } catch (IOException e) {
            LOGGER.error("Error in saving artifact configuration", e);
            throw new WisdomAppRuntimeException("Error in saving artifact configuration");
        }
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
                manager = new WisdomManager();
            } catch (IOException e) {
                e.printStackTrace(System.err);
                return;
            }
            Spark.port(managerPort);
            Spark.exception(WisdomServiceException.class, new WisdomServiceExceptionHandler());
            Spark.exception(InvalidPropertyException.class, new InvalidPropertyExceptionHandler());
            Spark.exception(NullPointerException.class, new InvalidPropertyExceptionHandler());
            Spark.exception(WisdomParserException.class, new WisdomParserExceptionHandler());
            Spark.exception(WisdomAppRuntimeException.class, new WisdomAppRuntimeExceptionHandler());
            Spark.exception(JsonSyntaxException.class, new JsonSyntaxExceptionHandler());
            Spark.post("/wisdom/deploy", (request, response) -> {
                Map<String, Object> deployer = manager.gson.fromJson(request.body(), Map.class);
                Utility.validateProperties(deployer, new String[]{"query", "port"}, new Class[]{String.class, Number.class});
                String query = (String) deployer.get("query");
                int port = ((Number) deployer.get("port")).intValue();
                manager.deploy(query, port);
                response.type(MEDIA_TEXT_PLAIN);
                response.status(HTTP_CREATED);
                return "Deployed Wisdom app successfully";
            });
            Spark.post("/wisdom/start/:appName", (request, response) -> manager.start(request.params("appName")));
            Spark.post("/wisdom/stop/:appName", (request, response) -> manager.stop(request.params("appName")));
            Spark.delete("/wisdom/app/:appName", (request, response) -> manager.delete(request.params("appName")));
            Spark.get("/wisdom/app/:appName", (request, response) -> manager.info(request.params("appName")), manager.gson::toJson);
        } catch (ArgumentParserException e) {
            e.printStackTrace(System.err);
        }
    }
}
