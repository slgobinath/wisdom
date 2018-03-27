package com.javahelps.wisdom.manager.entity;

import java.util.HashMap;
import java.util.Map;

public class Artifact {

    private int port;
    private long pid = -1L;
    private String file;
    private String host = "127.0.0.1";
    private int priority = 10;
    private Map<String, Map<String, Comparable>> init = new HashMap<>();

    public Artifact() {

    }

    public Artifact(Map<String, Object> map) {
        this.port = (int) map.get("port");
        this.file = (String) map.get("file");
        this.host = (String) map.get("host");
        this.priority = (int) map.get("priority");
        this.pid = ((Number) map.getOrDefault("pid", -1L)).longValue();
        this.init = (Map<String, Map<String, Comparable>>) map.getOrDefault("init", this.init);
    }

    public void setHost(String host) {
        this.host = host;
    }

    public String getHost() {
        return host;
    }

    public long getPid() {
        return pid;
    }

    public void setPid(long pid) {
        this.pid = pid;
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

    public int getPriority() {
        return priority;
    }

    public void setPriority(int priority) {
        if (priority < 0) {
            priority = 0;
        } else if (priority > 10) {
            priority = 10;
        }
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
