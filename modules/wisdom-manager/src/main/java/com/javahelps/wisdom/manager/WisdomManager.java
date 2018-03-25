package com.javahelps.wisdom.manager;

import com.javahelps.wisdom.core.WisdomApp;
import com.javahelps.wisdom.core.variable.Variable;
import com.javahelps.wisdom.query.WisdomCompiler;
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
import java.util.Properties;

import static com.javahelps.wisdom.core.util.WisdomConstants.THRESHOLD_STREAM;
import static com.javahelps.wisdom.manager.util.Constants.*;

public class WisdomManager {

    private final Yaml yaml;
    private final Path artifactsDirectory;
    private final Path configDirectory;
    private final Path artifactsConfigFile;

    public WisdomManager(Properties properties) throws IOException {
        Representer representer = new Representer();
        representer.addClassTag(Artifact.class, Tag.MAP);
        this.yaml = new Yaml(representer);
        this.artifactsDirectory = Paths.get(properties.getProperty(ARTIFACTS_DIR));
        this.configDirectory = Paths.get(properties.getProperty(CONF_DIR));
        this.artifactsConfigFile = this.configDirectory.resolve(ARTIFACTS_CONFIG_FILE);
        if (!Files.exists(this.artifactsDirectory)) {
            Files.createDirectories(this.artifactsDirectory);
        }
        if (!Files.exists(this.configDirectory)) {
            Files.createDirectories(this.configDirectory);
        }
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

        Map<String, Artifact> artifactMap = this.loadArtifactsConfig();
        if (artifactMap == null) {
            artifactMap = new HashMap<>();
        }
        artifactMap.put(app.getName(), artifact);

        System.out.println(this.yaml.dumpAsMap(artifact));
        this.deploy(path, artifactMap);
    }

    public void deploy(Path path, int port) throws IOException {
        this.deploy(path, port, Artifact.Priority.HIGH);
    }

    private Map<String, Artifact> loadArtifactsConfig() throws IOException {
        if (!Files.exists(this.artifactsConfigFile)) {
            Files.createFile(this.artifactsConfigFile);
        }
        Map<String, Artifact> artifactMap = new HashMap<>();
        try (BufferedReader reader = Files.newBufferedReader(this.artifactsConfigFile)) {
            Map<String, Map<String, Object>> config = this.yaml.load(reader);
            if (config != null) {
                for (Map.Entry<String, Map<String, Object>> entry : config.entrySet()) {
                    artifactMap.put(entry.getKey(), new Artifact(entry.getValue()));
                }
            }
        }
        return artifactMap;
    }

    private void deploy(Path wisdomQuery, Map<String, Artifact> artifactMap) throws IOException {
        Path target = this.artifactsDirectory.resolve(wisdomQuery.getFileName());
        try {
            Files.copy(wisdomQuery, target, StandardCopyOption.REPLACE_EXISTING);
            try (BufferedWriter writer = Files.newBufferedWriter(this.artifactsConfigFile)) {
                this.yaml.dump(artifactMap, writer);
            }
        } catch (IOException e) {
            Files.deleteIfExists(target);
        }
    }
}
