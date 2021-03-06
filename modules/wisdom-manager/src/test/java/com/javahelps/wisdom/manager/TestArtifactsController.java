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

package com.javahelps.wisdom.manager;

import com.javahelps.wisdom.manager.artifact.Artifact;
import com.javahelps.wisdom.manager.artifact.ArtifactController;
import com.javahelps.wisdom.manager.util.Utility;
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

public class TestArtifactsController {

    private static final Logger LOGGER = LoggerFactory.getLogger(TestArtifactsController.class);
    private static Yaml yaml;
    private static Map<String, Object> configuration;

    @BeforeClass
    public static void init() throws IOException, URISyntaxException {
        Representer representer = new Representer();
        representer.addClassTag(Artifact.class, Tag.MAP);
        yaml = new Yaml(representer);
        Files.createDirectory(Paths.get("conf"));
        Files.copy(Paths.get(ClassLoader.getSystemClassLoader().getResource("conf/config.yaml").toURI()), Paths.get("conf/config.yaml"));
        configuration = Utility.readYaml(yaml, Paths.get("conf/config.yaml"), false);
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

        LOGGER.info("Test deploy wisdom app");

        ArtifactController controller = new ArtifactController(Paths.get("."), Paths.get("./conf/artifacts.yaml"), configuration, yaml);
        controller.deploy(new String(Files.readAllBytes(Paths.get(ClassLoader.getSystemClassLoader().getResource("artifacts/ip_sweep.wisdomql").toURI()))), 8889);

        Map<String, Map<String, Object>> artifactMap;
        try (BufferedReader reader = Files.newBufferedReader(Paths.get("conf/artifacts.yaml"))) {
            artifactMap = yaml.load(reader);
        }
        System.out.println(artifactMap);

        Assert.assertTrue("Failed to deploy the query", Files.exists(Paths.get("artifacts/IPSweepDetector.wisdomql")));
        Assert.assertEquals("Invalid file name", "IPSweepDetector", artifactMap.get("IPSweepDetector").get("name"));
    }
}
