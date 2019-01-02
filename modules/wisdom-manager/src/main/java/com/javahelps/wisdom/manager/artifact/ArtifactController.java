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

package com.javahelps.wisdom.manager.artifact;

import com.javahelps.wisdom.core.WisdomApp;
import com.javahelps.wisdom.core.exception.WisdomAppRuntimeException;
import com.javahelps.wisdom.core.extension.ImportsManager;
import com.javahelps.wisdom.core.variable.Variable;
import com.javahelps.wisdom.dev.client.Response;
import com.javahelps.wisdom.dev.client.WisdomAdminClient;
import com.javahelps.wisdom.dev.client.WisdomClient;
import com.javahelps.wisdom.dev.client.WisdomHTTPClient;
import com.javahelps.wisdom.manager.util.Utility;
import com.javahelps.wisdom.query.WisdomCompiler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Yaml;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

import static com.javahelps.wisdom.core.util.Commons.map;
import static com.javahelps.wisdom.core.util.WisdomConstants.*;
import static com.javahelps.wisdom.dev.util.Constants.HTTP_OK;
import static com.javahelps.wisdom.manager.util.Constants.ARTIFACTS_DIR;

public class ArtifactController {

    private static final Logger LOGGER = LoggerFactory.getLogger(ArtifactController.class);

    static {
        ImportsManager.INSTANCE.scanClassPath();
    }

    private final Yaml yaml;
    private final File wisdomHome;
    private final Path artifactsPath;
    private final int minServicePort;
    private final int maxServicePort;

    private final Path artifactsConfigPath;
    private final Map<String, Artifact> deployedArtifacts = new HashMap<>();
    private final String JAVA_BIN = Paths.get(System.getProperty("java.home"), "bin/java").toAbsolutePath().toString();

    public ArtifactController(Path wisdomHome, Path artifactsConfigPath, Map<String, Object> configuration, Yaml yaml) throws IOException {

        this.wisdomHome = wisdomHome.toFile();
        this.artifactsPath = wisdomHome.resolve(ARTIFACTS_DIR);
        this.artifactsConfigPath = artifactsConfigPath;
        this.yaml = yaml;

        if (!Files.exists(this.artifactsPath)) {
            Files.createDirectories(this.artifactsPath);
        }

        Map<String, Object> wisdomService = (Map<String, Object>) configuration.get("wisdom_service");
        if (wisdomService == null) {
            wisdomService = Collections.emptyMap();
        }
        this.minServicePort = (int) wisdomService.getOrDefault("min_port", 8080);
        this.maxServicePort = (int) wisdomService.getOrDefault("max_port", 8888);
    }

    public void start() {
        this.loadArtifactsConfig();
        for (Artifact artifact : this.deployedArtifacts.values()) {
            LOGGER.info("Loaded artifact {}", artifact.getName());
            if (artifact.getPid() != -1 || artifact.isStoppedByManager()) {
                this.start(artifact);
            }
        }
    }

    public void deploy(String query, int port) throws IOException {

        Objects.requireNonNull(query, "query is not provided");
        // Port must be within range
        if (port < this.minServicePort || port > this.maxServicePort) {
            throw new WisdomAppRuntimeException("Wisdom service port must be within %d - %d but found %d", this.minServicePort, this.maxServicePort, port);
        }

        WisdomApp app = WisdomCompiler.parse(query);
        String appName = app.getName();

        // Port should not be assigned to existing app
        for (Artifact artifact : this.deployedArtifacts.values()) {
            if (port == artifact.getPort() && !appName.equals(artifact.getName())) {
                throw new WisdomAppRuntimeException("Port %d is already assigned to another Wisdom app: %s", port, artifact.getName());
            }
        }

        Properties appProperties = app.getProperties();
        Artifact artifact = new Artifact();
        artifact.setName(appName);
        artifact.setPort(port);
        artifact.setPriority(((Long) appProperties.getOrDefault(PRIORITY, 10L)).intValue());
        artifact.addRequires((Comparable[]) appProperties.get(REQUIRES));

        List<Variable> trainableVariables = app.getTrainable();
        for (Variable variable : trainableVariables) {
            artifact.addInit(THRESHOLD_STREAM, variable.getId(), (Comparable) variable.get());
        }
        deployedArtifacts.put(appName, artifact);
        Path target = this.artifactsPath.resolve(appName + ".wisdomql");
        try {
            Files.write(target, query.getBytes());
            this.saveArtifactsConfig();
        } catch (IOException e) {
            Files.deleteIfExists(target);
            throw new WisdomAppRuntimeException("Failed to deploy " + appName, e);
        }
    }

    public String start(String appName) {

        Objects.requireNonNull(appName, "Wisdom appName is not provided");
        Artifact artifact = deployedArtifacts.get(appName);
        if (artifact == null) {
            throw new WisdomAppRuntimeException("Wisdom app: '%s' not found in deployed applications", appName);
        }
        return this.start(artifact);
    }

    public String start(Artifact artifact) {

        String appName = artifact.getName();
        synchronized (artifact) {
            // Test if it is already running
            try (WisdomAdminClient client = new WisdomAdminClient(artifact.getHost(), artifact.getPort())) {
                Map<String, Comparable> info = client.info();
                if (info != null) {
                    if (info != null) {
                        return String.format("Wisdom app '%s' is already running on %d for %.2f seconds", appName, ((Number) info.get("port")).intValue(), ((Number) info.get("uptime")).doubleValue() / 1000);
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
                    this.artifactsPath.resolve(artifact.getFileName()).toAbsolutePath().toString()
            };
            ProcessBuilder builder = new ProcessBuilder(command);
            builder.directory(this.wisdomHome);
            Process process;
            try {
                process = builder.start();
            } catch (IOException e) {
                LOGGER.error("Failed to start wisdom service for " + appName, e);
                throw new WisdomAppRuntimeException("Failed to start wisdom service");
            }
            artifact.setPid(process.pid());
        }
        this.saveArtifactsConfig();
        // Initialize the artifact
        this.initialize(artifact);
        return "Application started successfully";
    }

    public String stop(String appName) {

        Objects.requireNonNull(appName, "Wisdom appName is not provided");
        Artifact artifact = deployedArtifacts.get(appName);
        if (artifact == null) {
            throw new WisdomAppRuntimeException("Wisdom app: '%s' not found in deployed applications", appName);
        }

        return this.stop(artifact);
    }

    public String stop(Artifact artifact) {

        // Test if it is already running
        try (WisdomAdminClient client = new WisdomAdminClient(artifact.getHost(), artifact.getPort())) {
            Map<String, Comparable> info = client.info();
            if (info == null) {
                return String.format("Wisdom app %s is not running", artifact.getName());
            } else {
                client.stop();
            }
        } catch (IOException e) {
            // Not running
        }
        artifact.setPid(-1L);
        this.saveArtifactsConfig();
        return String.format("Wisdom app %s is successfully stopped", artifact.getName());
    }

    public String delete(String appName) {

        this.stop(appName);
        // No need to check for null after this.stop
        Artifact artifact = deployedArtifacts.remove(appName);
        String fileName = artifact.getFileName();
        try {
            Files.deleteIfExists(this.artifactsPath.resolve(fileName));
            this.saveArtifactsConfig();
        } catch (IOException e) {
            LOGGER.error("Error in deleting " + fileName, e);
            throw new WisdomAppRuntimeException("Error in deleting " + fileName);
        }
        return String.format("Wisdom app %s is successfully deleted", appName);
    }

    public Map<String, Object> info(String appName) {

        Objects.requireNonNull(appName, "Wisdom appName is not provided");
        Artifact artifact = deployedArtifacts.get(appName);
        if (artifact == null) {
            throw new WisdomAppRuntimeException("Wisdom app: '%s' not found in deployed applications", appName);
        }

        Map<String, Object> info = map("name", appName, "port", artifact.getPort(), "pid", artifact.getPid(), "running", false);

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

    public String initialize(String appName, Map<String, Map<String, Comparable>> values) {

        Objects.requireNonNull(appName, "Wisdom appName is not provided");
        Objects.requireNonNull(values, "values are not not provided");
        Artifact artifact = deployedArtifacts.get(appName);
        if (artifact == null) {
            throw new WisdomAppRuntimeException("Wisdom app: '%s' not found in deployed applications", appName);
        }
        Map<String, Map<String, Object>> trainableStreams = artifact.getInit();
        for (Map.Entry<String, Map<String, Comparable>> entry : values.entrySet()) {
            Map<String, Object> variables = trainableStreams.get(entry.getKey());
            if (variables != null) {
                for (Map.Entry<String, Comparable> varMapping : entry.getValue().entrySet()) {
                    variables.replace(varMapping.getKey(), varMapping.getValue());
                }
            }
        }
        this.saveArtifactsConfig();
        this.initialize(artifact);
        return "Initialized the artifact " + appName;
    }

    public List<Map<String, Object>> info() {
        List<Map<String, Object>> allInfo = new ArrayList<>(this.deployedArtifacts.size());
        for (String appName : this.deployedArtifacts.keySet()) {
            allInfo.add(this.info(appName));
        }
        return allInfo;
    }

    public String initialize(Artifact artifact) {

        // Test if it is already running
        try (WisdomClient client = new WisdomHTTPClient(artifact.getHost(), artifact.getPort())) {
            // Initialize all variables
            for (Map.Entry<String, Map<String, Object>> entry : artifact.getInit().entrySet()) {
                Response response = client.send(entry.getKey(), entry.getValue());
                if (response.getStatus() != HTTP_OK) {
                    return response.getReason();
                }
            }
        } catch (IOException e) {
            // Not running
        }
        return String.format("Wisdom app %s is initialized successfully", artifact.getName());
    }

    public void shutdown() {
        for (Artifact artifact : this.deployedArtifacts.values()) {
            if (artifact.getPid() != -1) {
                LOGGER.info("Stopping artifact {}", artifact.getName());
                this.stop(artifact);
                artifact.setStoppedByManager(true);
            }
        }
        this.saveArtifactsConfig();
    }

    private synchronized void loadArtifactsConfig() {
        this.deployedArtifacts.clear();
        Map<String, Object> config = Utility.readYaml(this.yaml, this.artifactsConfigPath, true);
        System.out.println(config);
        for (Map.Entry<String, Object> entry : config.entrySet()) {
            this.deployedArtifacts.put(entry.getKey(), new Artifact((Map<String, Object>) entry.getValue()));
        }
    }

    public synchronized void saveArtifactsConfig() {
        try (BufferedWriter writer = Files.newBufferedWriter(this.artifactsConfigPath)) {
            this.yaml.dump(deployedArtifacts, writer);
        } catch (IOException e) {
            LOGGER.error("Error in saving artifact configuration", e);
            throw new WisdomAppRuntimeException("Error in saving artifact configuration");
        }
    }

    public Artifact getArtifact(String name) {
        return this.deployedArtifacts.get(name);
    }

    public Artifact getArtifactRequires(String streamName) {

        for (Artifact artifact : this.deployedArtifacts.values()) {
            if (artifact.getRequires().contains(streamName)) {
                return artifact;
            }
        }
        return null;
    }


    public Iterable<Artifact> getArtifacts() {
        return this.deployedArtifacts.values();
    }
}
