package com.javahelps.wisdom.dev.util;

import java.util.HashMap;
import java.util.Map;

public class EventUtil {

    private EventUtil() {

    }

    public static Map<String, Comparable> map(Comparable... entries) {
        int count = entries.length;
        Map<String, Comparable> map = new HashMap<>(count / 2);
        for (int i = 0; i < count; i += 2) {
            map.put((String) entries[i], entries[i + 1]);
        }
        return map;
    }
}
