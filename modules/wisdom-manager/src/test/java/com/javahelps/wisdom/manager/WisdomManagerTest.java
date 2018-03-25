package com.javahelps.wisdom.manager;

import com.javahelps.wisdom.core.extension.ImportsManager;
import com.javahelps.wisdom.extensions.unique.window.UniqueExternalTimeBatchWindow;
import com.javahelps.wisdom.service.sink.HTTPSink;
import com.javahelps.wisdom.service.sink.KafkaSink;
import com.javahelps.wisdom.service.source.HTTPSource;
import com.javahelps.wisdom.service.source.KafkaSource;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.nodes.Tag;
import org.yaml.snakeyaml.representer.Representer;

import java.io.BufferedReader;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Map;
import java.util.Properties;

import static com.javahelps.wisdom.manager.util.Constants.ARTIFACTS_DIR;
import static com.javahelps.wisdom.manager.util.Constants.CONF_DIR;

public class WisdomManagerTest {

    static {
        // Supported sources
        ImportsManager.INSTANCE.use(HTTPSource.class);
        ImportsManager.INSTANCE.use(KafkaSource.class);
        // Supported sinks
        ImportsManager.INSTANCE.use(HTTPSink.class);
        ImportsManager.INSTANCE.use(KafkaSink.class);

        ImportsManager.INSTANCE.use(UniqueExternalTimeBatchWindow.class);
    }

    private static final Logger LOGGER = LoggerFactory.getLogger(WisdomManagerTest.class);
    private static Yaml yaml;

    @BeforeClass
    public static void init() {
        Representer representer = new Representer();
        representer.addClassTag(Artifact.class, Tag.MAP);
        yaml = new Yaml(representer);
    }

    @AfterClass
    public static void clean() throws IOException {
        Files.deleteIfExists(Paths.get("artifacts/ip_sweep.wisdomql"));
        Files.deleteIfExists(Paths.get("conf/artifacts.yaml"));
        Files.deleteIfExists(Paths.get("artifacts"));
        Files.deleteIfExists(Paths.get("conf"));
    }

    @Test
    public void testDeployWisdomApp() throws URISyntaxException, IOException {

        LOGGER.info("Test query file");

        Properties properties = new Properties();
        properties.setProperty(ARTIFACTS_DIR, "artifacts");
        properties.setProperty(CONF_DIR, "conf");

        WisdomManager manager = new WisdomManager(properties);
        manager.deploy(Paths.get(ClassLoader.getSystemClassLoader().getResource("artifacts/ip_sweep.wisdomql").toURI()), 8080);

        Map<String, Map<String, Object>> artifactMap;
        try (BufferedReader reader = Files.newBufferedReader(Paths.get("conf/artifacts.yaml"))) {
            artifactMap = yaml.load(reader);
        }
        System.out.println(artifactMap);

        Assert.assertTrue("Failed to copy the query file", Files.exists(Paths.get("artifacts/ip_sweep.wisdomql")));
        Assert.assertEquals("Invalid file name", "ip_sweep.wisdomql", artifactMap.get("IPSweepDetector").get("file"));
    }
}
