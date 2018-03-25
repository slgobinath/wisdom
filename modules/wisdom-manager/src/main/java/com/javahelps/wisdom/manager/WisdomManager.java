package com.javahelps.wisdom.manager;

import com.javahelps.wisdom.core.WisdomApp;
import com.javahelps.wisdom.core.exception.WisdomAppRuntimeException;
import com.javahelps.wisdom.core.variable.Variable;
import com.javahelps.wisdom.manager.optimize.multivariate.Point;
import com.javahelps.wisdom.manager.optimize.wisdom.QueryTrainer;
import com.javahelps.wisdom.manager.optimize.wisdom.WisdomOptimizer;
import com.javahelps.wisdom.query.WisdomCompiler;
import com.javahelps.wisdom.service.client.WisdomAdminClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.nodes.Tag;
import org.yaml.snakeyaml.representer.Representer;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;

import static com.javahelps.wisdom.core.util.WisdomConstants.THRESHOLD_STREAM;
import static com.javahelps.wisdom.manager.util.Constants.*;

public class WisdomManager {

    private static final Logger LOGGER = LoggerFactory.getLogger(WisdomManager.class);

    private final Yaml yaml;
    private final Path ARTIFACTS_PATH;
    private final Path CONFIG_DIR_PATH;
    private final Path ARTIFACTS_CONFIG_PATH;
    private final Map<String, Artifact> DEPLOYED_ARTIFACTS = new HashMap<>();
    private final Path WISDOM_HOME_PATH;
    private final ScheduledExecutorService scheduledExecutorService;
    private final ExecutorService executorService;

    public WisdomManager() throws IOException {
        this(System.getenv(WISDOM_HOME));
    }

    public WisdomManager(String wisdomHome) throws IOException {

        if (wisdomHome == null) {
            throw new WisdomAppRuntimeException("%s environment variable not set", WISDOM_HOME);
        }
        this.WISDOM_HOME_PATH = Paths.get(wisdomHome);

        Representer representer = new Representer();
        representer.addClassTag(Artifact.class, Tag.MAP);
        this.yaml = new Yaml(representer);
        this.ARTIFACTS_PATH = WISDOM_HOME_PATH.resolve(Paths.get(ARTIFACTS_DIR));
        this.CONFIG_DIR_PATH = WISDOM_HOME_PATH.resolve(Paths.get(CONF_DIR));
        this.ARTIFACTS_CONFIG_PATH = this.CONFIG_DIR_PATH.resolve(ARTIFACTS_CONFIG_FILE);
        if (!Files.exists(this.ARTIFACTS_PATH)) {
            Files.createDirectories(this.ARTIFACTS_PATH);
        }
        if (!Files.exists(this.CONFIG_DIR_PATH)) {
            Files.createDirectories(this.CONFIG_DIR_PATH);
        }
        this.loadArtifactsConfig();
        this.scheduledExecutorService = Executors.newScheduledThreadPool(4);
        this.executorService = Executors.newCachedThreadPool();
    }

    public void deploy(Path path, int port) throws IOException {
        this.deploy(path, port, Artifact.Priority.HIGH);
    }

    public void deploy(Path path, int port, Artifact.Priority priority) throws IOException {

        WisdomApp app = WisdomCompiler.parse(path);

        Artifact artifact = new Artifact();
        artifact.setFile(path.getFileName().toString());
        artifact.setPort(port);
        artifact.setPriority(priority);

        List<Variable> trainableVariables = app.getTrainable();
        for (Variable variable : trainableVariables) {
            artifact.addInit(THRESHOLD_STREAM, variable.getId(), (Comparable) variable.get());
        }
        DEPLOYED_ARTIFACTS.put(app.getName(), artifact);
        this.deploy(path);
    }

    public void start(String appName) throws IOException {

        Artifact artifact = DEPLOYED_ARTIFACTS.get(appName);
        if (artifact == null) {
            LOGGER.error("Wisdom app: {} not found in deployed applications", appName);
            return;
        }
        String[] command = {
                "wisdom-service.sh",
                "--port",
                Integer.toString(artifact.getPort()),
                this.ARTIFACTS_PATH.resolve(artifact.getFile()).toAbsolutePath().toString()
        };
        ProcessBuilder builder = new ProcessBuilder(command);
        builder.directory(this.WISDOM_HOME_PATH.toFile());
        Process process = builder.start();
        artifact.setPid(process.pid());
        this.saveArtifactsConfig();
    }

    public void stop(String appName) throws IOException {

        Artifact artifact = DEPLOYED_ARTIFACTS.get(appName);
        if (artifact == null) {
            LOGGER.error("Wisdom app: {} not found in deployed applications", appName);
            return;
        }
        WisdomAdminClient client = new WisdomAdminClient(artifact.getHost(), artifact.getPort());
        client.stop();
        artifact.setPid(-1L);
        this.saveArtifactsConfig();
    }

    public void optimize(String appName, QueryTrainer... queryTrainers) throws IOException {

        Artifact artifact = DEPLOYED_ARTIFACTS.get(appName);
        if (artifact == null) {
            LOGGER.error("Wisdom app: {} not found in deployed applications", appName);
            return;
        }
        WisdomApp app = WisdomCompiler.parse(this.ARTIFACTS_PATH.resolve(Paths.get(artifact.getFile())));
        Callable<Point> callable = WisdomOptimizer.callable(app, queryTrainers);
        Future<Point> future = this.executorService.submit(callable);
    }

    private void loadArtifactsConfig() throws IOException {
        if (!Files.exists(this.ARTIFACTS_CONFIG_PATH)) {
            Files.createFile(this.ARTIFACTS_CONFIG_PATH);
        }
        DEPLOYED_ARTIFACTS.clear();
        try (BufferedReader reader = Files.newBufferedReader(this.ARTIFACTS_CONFIG_PATH)) {
            Map<String, Map<String, Object>> config = this.yaml.load(reader);
            if (config != null) {
                for (Map.Entry<String, Map<String, Object>> entry : config.entrySet()) {
                    DEPLOYED_ARTIFACTS.put(entry.getKey(), new Artifact(entry.getValue()));
                }
            }
        }
    }

    private void saveArtifactsConfig() throws IOException {
        try (BufferedWriter writer = Files.newBufferedWriter(this.ARTIFACTS_CONFIG_PATH)) {
            this.yaml.dump(DEPLOYED_ARTIFACTS, writer);
        }
    }

    private void deploy(Path wisdomQuery) throws IOException {
        Path target = this.ARTIFACTS_PATH.resolve(wisdomQuery.getFileName());
        try {
            Files.copy(wisdomQuery, target, StandardCopyOption.REPLACE_EXISTING);
            this.saveArtifactsConfig();
        } catch (IOException e) {
            Files.deleteIfExists(target);
        }
    }
}
