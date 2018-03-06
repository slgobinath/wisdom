package com.javahelps.wisdom.query.tree;

import java.util.Properties;

public class Annotation {

    private final String name;
    private final Properties properties;

    public Annotation(String name) {
        this(name, new Properties());
    }

    public Annotation(String name, Properties properties) {
        this.name = name;
        this.properties = properties;
    }

    public String getName() {
        return name;
    }

    public <T> T getProperty(String key) {
        return (T) this.properties.get(key);
    }

    public <T> void setProperty(String key, T value) {
        this.properties.put(key, value);
    }

    public boolean hasProperty(String key) {
        return this.properties.containsKey(key);
    }

    public Properties getProperties() {
        return properties;
    }

    @Override
    public String toString() {
        return String.format("@%s%s", this.name, this.properties);
    }
}
