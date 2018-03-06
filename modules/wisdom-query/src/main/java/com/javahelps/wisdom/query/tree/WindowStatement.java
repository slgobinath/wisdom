package com.javahelps.wisdom.query.tree;

import com.javahelps.wisdom.core.query.Query;
import com.javahelps.wisdom.core.window.Window;

import java.util.HashMap;
import java.util.Map;

public class WindowStatement implements Statement {

    private final String name;
    private String type;
    private Map<String, Comparable> properties = new HashMap<>();
    private int count = 0;


    public WindowStatement(String name) {
        this.name = name;
    }

    public void setType(String type) {
        this.type = type;
    }

    public void addProperty(String key, Comparable value) {
        if (key == null) {
            key = String.format("_param_%d", count);
        }
        this.properties.put(key, value);
        count++;
    }

    @Override
    public void addTo(Query query) {
        String namespace = type == null ? name : name + ":" + type;
        Window window = Window.create(namespace, this.properties);
        query.window(window);
    }
}
