package com.javahelps.wisdom.core;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by gobinath on 7/10/17.
 */
public class TestUtil {

    public static Map<String, Comparable> map(Comparable... entries) {
        int count = entries.length;
        Map<String, Comparable> map = new HashMap<>(count / 2);
        for (int i = 0; i < count; i += 2) {
            map.put((String) entries[i], entries[i + 1]);
        }
        return map;
    }

}
