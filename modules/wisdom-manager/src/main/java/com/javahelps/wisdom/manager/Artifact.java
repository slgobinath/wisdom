package com.javahelps.wisdom.manager;

import java.util.HashMap;
import java.util.Map;

public class Artifact {

    enum Priority {
        LOW, HIGH
    }

    private int port;
    private String file;
    private Priority priority;
    private Map<String, Map<String, Comparable>> init = new HashMap<>();

    public Artifact() {

    }

    public Artifact(Map<String, Object> map) {
        this.port = (int) map.get("port");
        this.file = (String) map.get("file");
        this.priority = Priority.valueOf((String) map.get("priority"));
        this.init = (Map<String, Map<String, Comparable>>) map.getOrDefault("init", this.init);
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public String getFile() {
        return file;
    }

    public void setFile(String file) {
        this.file = file;
    }

    public Priority getPriority() {
        return priority;
    }

    public void setPriority(Priority priority) {
        this.priority = priority;
    }

    public Map<String, Map<String, Comparable>> getInit() {
        return init;
    }

    public void setInit(Map<String, Map<String, Comparable>> init) {
        this.init = init;
    }

    public void addInit(String streamId, String variableId, Comparable value) {
        Map<String, Comparable> map = this.init.get(streamId);
        if (map == null) {
            map = new HashMap<>();
            this.init.put(streamId, map);
        }
        map.put(variableId, value);
    }
}
