package com.javahelps.wisdom.manager.stats;

import com.javahelps.wisdom.core.exception.WisdomAppRuntimeException;
import com.javahelps.wisdom.manager.artifact.Artifact;
import com.javahelps.wisdom.manager.artifact.ArtifactController;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static com.javahelps.wisdom.manager.util.Constants.*;

public class StatisticsManager implements StatisticsConsumer.StatsListener {

    private static final Logger LOGGER = LoggerFactory.getLogger(StatisticsManager.class);

    private final long initialDelay;
    private final long monitorPeriod;
    private final ArtifactController controller;
    private final ExecutorService executorService;
    private final StatisticsConsumer statisticsConsumer;
    private final ScheduledExecutorService scheduledExecutorService;

    public StatisticsManager(String managerId, ArtifactController controller, Map<String, Object> configuration) {
        configuration = (Map<String, Object>) configuration.get("statistics");
        if (configuration == null) {
            throw new WisdomAppRuntimeException("No statistics configuration in config file");
        }
        this.controller = controller;
        this.executorService = Executors.newCachedThreadPool();
        this.scheduledExecutorService = Executors.newScheduledThreadPool(4);
        this.initialDelay = ((Number) configuration.getOrDefault("delay", STATISTIC_MONITOR_INITIAL_DELAY)).longValue();
        this.monitorPeriod = ((Number) configuration.getOrDefault("period", STATISTIC_MONITOR_PERIOD)).longValue();
        this.statisticsConsumer = new StatisticsConsumer((String) configuration.get("kafka_bootstrap"), (String) configuration.get("kafka_topic"), managerId, this.executorService, this);
    }

    @Override
    public void onStats(Map<String, Comparable> stats) {
        LOGGER.info("Received statistics {}", stats);
        String appName = (String) stats.get("app");
        if (appName != null) {
            Artifact artifact = this.controller.getArtifact((String) stats.get("app"));
            if (artifact != null) {
                double throughput = ((Number) stats.getOrDefault("throughput", 0.0)).doubleValue();
                artifact.addThroughput(throughput);
                if (artifact.getPid() == -1) {
                    artifact.setPid(1); // Started by someone else
                }
                this.controller.saveArtifactsConfig();
            }
        }
    }

    public void start() {
        this.statisticsConsumer.start();
        this.scheduledExecutorService.scheduleAtFixedRate(this::manageRunningInstances, this.initialDelay, this.monitorPeriod, TimeUnit.MILLISECONDS);
    }

    public void stop() {
        this.statisticsConsumer.stop();
        this.executorService.shutdown();
        this.scheduledExecutorService.shutdown();
    }

    public void manageRunningInstances() {
        boolean needResource = false;
        List<Artifact> stopable = new ArrayList<>();
        List<Artifact> stopped = new ArrayList<>();
        for (Artifact artifact : this.controller.getArtifacts()) {
            if (artifact.getPid() != -1L) {
                // Running
                double throughput = artifact.averageThroughput();
                if (throughput >= MAXIMUM_THROUGHPUT_THRESHOLD) {
                    needResource = true;
                } else if (throughput <= MINIMUM_THROUGHPUT_THRESHOLD && artifact.getPriority() <= MINIMUM_PRIORITY_THRESHOLD) {
                    stopable.add(artifact);
                }
            } else if (artifact.isStoppedByManager()) {
                stopped.add(artifact);
            }
        }
        if (needResource) {
            // Stop possible artifacts
            for (Artifact artifact : stopable) {
                artifact.setStoppedByManager(true);
                this.controller.stop(artifact);
            }
        } else {
            // Restart stopped artifacts
            for (Artifact artifact : stopped) {
                artifact.setStoppedByManager(false);
                this.controller.start(artifact);
            }
        }
    }
}
