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
