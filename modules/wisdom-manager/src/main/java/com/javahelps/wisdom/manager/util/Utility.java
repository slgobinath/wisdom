package com.javahelps.wisdom.manager.util;

import com.javahelps.wisdom.core.exception.WisdomAppRuntimeException;
import com.javahelps.wisdom.manager.exception.InvalidPropertyException;
import com.javahelps.wisdom.manager.optimize.multivariate.Constraint;
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

    public static int getMinPos(double[] list) {
        int pos = 0;
        double minValue = list[0];

        for (int i = 1; i < list.length; i++) {
            if (list[i] < minValue) {
                pos = i;
                minValue = list[i];
            }
        }

        return pos;
    }

    public static Constraint[] velocityBound(Constraint... bounds) {
        int length = bounds.length;
        Constraint[] velocityBounds = new Constraint[length];
        for (int i = 0; i < length; i++) {
            double max = Math.abs(bounds[i].getHigh() - bounds[i].getLow());
            double min = -max;
            velocityBounds[i] = new Constraint(min, max);
        }
        return velocityBounds;
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
