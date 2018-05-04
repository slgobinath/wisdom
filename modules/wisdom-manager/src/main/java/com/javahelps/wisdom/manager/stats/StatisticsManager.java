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
    public void onStats(Map<String, Object> stats) {
        LOGGER.debug("Received statistics {}", stats);
        String streamName = (String) stats.get("name");
        if (streamName != null) {
            Artifact artifact = this.controller.getArtifactRequires(streamName);
            if (artifact != null) {
                double throughput = ((Number) stats.getOrDefault("throughput", 0.0)).doubleValue();
                artifact.addThroughput(throughput);
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
        List<Artifact> stopable = new ArrayList<>();
        List<Artifact> startable = new ArrayList<>();
        for (Artifact artifact : this.controller.getArtifacts()) {
            double throughput = artifact.averageThroughput();
            long pid = artifact.getPid();
            if (pid == -1L && throughput > MINIMUM_THROUGHPUT_THRESHOLD) {
                // Not running but need boosting
                startable.add(artifact);
            } else if (pid != -1L && throughput <= MINIMUM_THROUGHPUT_THRESHOLD && artifact.getPriority() <= MINIMUM_PRIORITY_THRESHOLD) {
                // Running but no longer required
                stopable.add(artifact);
            }
        }

        // Start possible artifacts
        for (Artifact artifact : startable) {
            LOGGER.info("Starting {}", artifact.getName());
            this.controller.start(artifact);
        }
        // Stop possible artifacts
        for (Artifact artifact : stopable) {
            LOGGER.info("Stopping {}", artifact.getName());
            this.controller.stop(artifact);
        }
    }
}
