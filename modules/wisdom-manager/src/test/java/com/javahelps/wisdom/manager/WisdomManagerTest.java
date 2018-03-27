package com.javahelps.wisdom.manager;

import com.javahelps.wisdom.manager.entity.Artifact;
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

public class WisdomManagerTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(WisdomManagerTest.class);
    private static Yaml yaml;

    @BeforeClass
    public static void init() throws IOException, URISyntaxException {
        Representer representer = new Representer();
        representer.addClassTag(Artifact.class, Tag.MAP);
        yaml = new Yaml(representer);
        Files.createDirectory(Paths.get("conf"));
        Files.copy(Paths.get(ClassLoader.getSystemClassLoader().getResource("conf/config.yaml").toURI()), Paths.get("conf/config.yaml"));
    }

    @AfterClass
    public static void clean() throws IOException {
        Files.deleteIfExists(Paths.get("artifacts/IPSweepDetector.wisdomql"));
        Files.deleteIfExists(Paths.get("conf/artifacts.yaml"));
        Files.deleteIfExists(Paths.get("conf/config.yaml"));
        Files.deleteIfExists(Paths.get("artifacts"));
        Files.deleteIfExists(Paths.get("conf"));
    }

    @Test
    public void testDeployWisdomApp() throws URISyntaxException, IOException {

        LOGGER.info("Test query file");

        WisdomManager manager = new WisdomManager(".");
        manager.deploy(new String(Files.readAllBytes(Paths.get(ClassLoader.getSystemClassLoader().getResource("artifacts/ip_sweep.wisdomql").toURI()))), 8889);

        Map<String, Map<String, Object>> artifactMap;
        try (BufferedReader reader = Files.newBufferedReader(Paths.get("conf/artifacts.yaml"))) {
            artifactMap = yaml.load(reader);
        }
        System.out.println(artifactMap);

        Assert.assertTrue("Failed to deploy the query", Files.exists(Paths.get("artifacts/IPSweepDetector.wisdomql")));
        Assert.assertEquals("Invalid file name", "IPSweepDetector.wisdomql", artifactMap.get("IPSweepDetector").get("file"));
    }
}
