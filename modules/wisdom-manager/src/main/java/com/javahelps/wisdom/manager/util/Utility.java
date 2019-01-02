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

package com.javahelps.wisdom.manager.util;

import com.javahelps.wisdom.core.exception.WisdomAppRuntimeException;
import com.javahelps.wisdom.manager.exception.InvalidPropertyException;
import org.yaml.snakeyaml.Yaml;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.Map;

public class Utility {

    private Utility() {

    }

    public static void validateProperties(Map<String, Object> map, String[] properties, Class<?>[] types) {
        int size = properties.length;
        if (size != types.length) {
            throw new IllegalArgumentException("Length of properties and types should be equal");
        }

        for (int i = 0; i < size; i++) {
            String key = properties[i];
            Object value = map.get(key);
            if (value == null) {
                throw new InvalidPropertyException("Property '%s' not found", key);
            } else if (!types[i].isAssignableFrom(value.getClass())) {
                throw new InvalidPropertyException("Expected property %s of type %s bout found %s", key, types[i], value.getClass());
            }
        }
    }

    public static Map<String, Object> readYaml(Yaml yaml, Path configPath, boolean createIfNotExist) {
        if (!Files.exists(configPath)) {
            if (createIfNotExist) {
                try {
                    Files.createFile(configPath);
                } catch (IOException e) {
                    throw new WisdomAppRuntimeException("Failed to create " + configPath, e);
                }
            } else {
                throw new WisdomAppRuntimeException("%s does not exist", configPath);
            }
        }
        try (BufferedReader reader = Files.newBufferedReader(configPath)) {
            Map<String, Object> map = yaml.load(reader);
            if (map == null) {
                map = Collections.emptyMap();
            }
            return map;
        } catch (IOException e) {
            throw new WisdomAppRuntimeException("Error in reading " + configPath, e);
        }
    }

}
