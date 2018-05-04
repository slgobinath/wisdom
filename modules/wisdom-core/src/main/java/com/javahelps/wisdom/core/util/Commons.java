package com.javahelps.wisdom.core.util;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class Commons {

    private Commons() {

    }

    public static Map<String, Object> map(Object... entries) {
        int count = entries.length;
        Map<String, Object> map = new HashMap<>(count / 2);
        for (int i = 0; i < count; i += 2) {
            map.put((String) entries[i], entries[i + 1]);
        }
        return map;
    }

    public static Properties toProperties(Object... entries) {
        int count = entries.length;
        Properties properties = new Properties();
        for (int i = 0; i < count; i += 2) {
            properties.put((String) entries[i], entries[i + 1]);
        }
        return properties;
    }

    public static <T> T getProperty(Map<String, ?> properties, String attribute, int index) {
        T value = (T) properties.get(attribute);
        if (value == null) {
            value = (T) properties.get(String.format("_param_%d", index));
        }
        return value;
    }
}
