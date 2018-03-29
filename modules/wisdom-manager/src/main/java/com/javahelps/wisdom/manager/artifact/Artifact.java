package com.javahelps.wisdom.manager.artifact;

import com.google.common.collect.EvictingQueue;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.javahelps.wisdom.manager.util.Constants.MINIMUM_THROUGHPUT_THRESHOLD;
import static com.javahelps.wisdom.manager.util.Constants.THROUGHPUT_WINDOW_LENGTH;

public class Artifact {

    private int port;
    private long pid = -1L;
    private String name;
    private String host = "127.0.0.1";
    private int priority = 10;
    private boolean stoppedByManager;
    private Map<String, Map<String, Comparable>> init = new HashMap<>();
    private final EvictingQueue<Double> throughputs = EvictingQueue.create(THROUGHPUT_WINDOW_LENGTH);

    public Artifact() {

    }

    public Artifact(Map<String, Object> map) {
        this.port = (int) map.get("port");
        this.name = (String) map.get("name");
        this.host = (String) map.get("host");
        this.priority = (int) map.get("priority");
        this.pid = ((Number) map.getOrDefault("pid", -1L)).longValue();
        this.init = (Map<String, Map<String, Comparable>>) map.getOrDefault("init", this.init);
        List<Double> throughputs = (List<Double>) map.get("throughputs");
        if (throughputs != null) {
            this.throughputs.addAll(throughputs);
        }
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

    public String getName() {
        return name;
    }

    public String getFileName() {
        return this.name + ".wisdomql";
    }

    public void setName(String name) {
        this.name = name;
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

    public void setStoppedByManager(boolean stoppedByManager) {
        this.stoppedByManager = stoppedByManager;
    }

    public boolean isStoppedByManager() {
        return stoppedByManager;
    }

    public EvictingQueue<Double> getThroughputs() {
        return throughputs;
    }

    public void addInit(String streamId, String variableId, Comparable value) {
        Map<String, Comparable> map = this.init.get(streamId);
        if (map == null) {
            map = new HashMap<>();
            this.init.put(streamId, map);
        }
        map.put(variableId, value);
    }

    public boolean isStopable(int priority) {
        if (priority > this.priority) {
            double totalThroughput = 0.0;
            for (Double throughput : this.throughputs) {
                totalThroughput += throughput;
            }
            return totalThroughput <= MINIMUM_THROUGHPUT_THRESHOLD;
        }
        return false;
    }

    public double averageThroughput() {
        double totalThroughput = 0.0;
        for (Double throughput : this.throughputs) {
            totalThroughput += throughput;
        }
        return totalThroughput / THROUGHPUT_WINDOW_LENGTH;
    }

    public void addThroughput(double throughput) {
        synchronized (this) {
            this.throughputs.offer(throughput);
        }
    }

    @Override
    public String toString() {
        return this.name;
    }
}
