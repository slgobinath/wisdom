package com.javahelps.wisdom.core;

import com.javahelps.wisdom.core.statistics.StatisticsManager;
import com.javahelps.wisdom.core.util.Scheduler;
import com.javahelps.wisdom.core.util.SystemTimestampGenerator;
import com.javahelps.wisdom.core.util.TimestampGenerator;
import com.javahelps.wisdom.core.util.WisdomConfig;

import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.*;

import static com.javahelps.wisdom.core.util.WisdomConstants.*;

/**
 * The user defined and system defined configurations of the stream processor are stored in {@link WisdomContext}.
 */
public class WisdomContext {

    private final boolean async;
    private final Scheduler scheduler;
    private final ExecutorService executorService;
    private final ThreadFactory threadFactory;
    private final ThreadBarrier threadBarrier;
    private StatisticsManager statisticsManager;
    private final TimestampGenerator timestampGenerator;
    private final ScheduledExecutorService scheduledExecutorService;
    private String statisticStream;
    private long reportFrequency;

    public WisdomContext(Properties properties) {
        this.timestampGenerator = new SystemTimestampGenerator();
        this.scheduledExecutorService = Executors.newScheduledThreadPool(4);
        this.scheduler = new Scheduler(this);
        this.executorService = Executors.newCachedThreadPool();
        this.threadFactory = Executors.defaultThreadFactory();
        this.threadBarrier = new ThreadBarrier();

        this.async = (boolean) properties.getOrDefault(ASYNC, WisdomConfig.ASYNC_ENABLED);
        this.statisticStream = (String) properties.get(STATISTICS);
        this.reportFrequency = (long) properties.getOrDefault(STATISTICS_REPORT_FREQUENCY, WisdomConfig.STATISTICS_REPORT_FREQUENCY);
        if (this.statisticStream != null) {
            this.statisticsManager = new StatisticsManager(this.statisticStream, this.reportFrequency);
        }
    }

    public void init(WisdomApp app) {
        if (this.statisticsManager != null) {
            this.statisticsManager.init(app);
        }
    }

    public TimestampGenerator getTimestampGenerator() {
        return timestampGenerator;
    }

    public ScheduledExecutorService getScheduledExecutorService() {
        return scheduledExecutorService;
    }

    public Scheduler getScheduler() {
        return scheduler;
    }

    public Executor getExecutorService() {
        return executorService;
    }

    public ThreadFactory getThreadFactory() {
        return threadFactory;
    }

    public ThreadBarrier getThreadBarrier() {
        return threadBarrier;
    }

    public StatisticsManager getStatisticsManager() {
        return statisticsManager;
    }

    public void start() {
        if (this.statisticsManager != null) {
            this.statisticsManager.start();
        }
    }

    public void shutdown() {
        this.scheduledExecutorService.shutdown();
        this.executorService.shutdown();
        if (this.statisticsManager != null) {
            this.statisticsManager.stop();
        }
    }

    public boolean isAsync() {
        return async;
    }

    public boolean isStatisticsEnabled() {
        return this.statisticStream != null;
    }

    public StatisticsManager enableStatistics(String statisticStream, long reportFrequency) {
        Objects.requireNonNull(statisticStream, "Statistic streamId cannot be null");
        this.statisticStream = statisticStream;
        this.reportFrequency = reportFrequency;
        if (this.isStatisticsEnabled()) {
            this.statisticsManager = new StatisticsManager(this.statisticStream, this.reportFrequency);
        }
        return this.statisticsManager;
    }

}
