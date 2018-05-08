package com.javahelps.wisdom.core;

import com.javahelps.wisdom.core.time.*;
import com.javahelps.wisdom.core.util.WisdomConfig;

import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;

import static com.javahelps.wisdom.core.util.WisdomConstants.ASYNC;
import static com.javahelps.wisdom.core.util.WisdomConstants.PLAYBACK;

/**
 * The user defined and system defined configurations of the stream processor are stored in {@link WisdomContext}.
 */
public class WisdomContext {

    private final boolean async;
    private final boolean playbackEnabled;
    private final String playbackAttribute;
    private final Scheduler scheduler;
    private final Properties properties;
    private final ExecutorService executorService;
    private final ThreadFactory threadFactory;
    private final ThreadBarrier threadBarrier;
    private final TimestampGenerator timestampGenerator;
    private final ScheduledExecutorService scheduledExecutorService;

    public WisdomContext(Properties properties) {
        this.properties = properties;
        this.scheduledExecutorService = Executors.newScheduledThreadPool(4);
        this.executorService = Executors.newCachedThreadPool();
        this.threadFactory = Executors.defaultThreadFactory();
        this.threadBarrier = new ThreadBarrier();
        this.async = (boolean) properties.getOrDefault(ASYNC, WisdomConfig.ASYNC_ENABLED);
        this.playbackAttribute = properties.getProperty(PLAYBACK);
        this.playbackEnabled = this.playbackAttribute != null;
        if (this.playbackEnabled) {
            this.timestampGenerator = new EventBasedTimestampGenerator();
            this.scheduler = new EventBasedScheduler(this.executorService, (EventBasedTimestampGenerator) this.timestampGenerator);
        } else {
            this.timestampGenerator = new SystemTimestampGenerator();
            this.scheduler = new SystemScheduler(this.scheduledExecutorService, this.timestampGenerator);
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

    public ExecutorService getExecutorService() {
        return executorService;
    }

    public ThreadFactory getThreadFactory() {
        return threadFactory;
    }

    public ThreadBarrier getThreadBarrier() {
        return threadBarrier;
    }

    public void start() {

    }

    public void shutdown() {
        this.scheduledExecutorService.shutdown();
        this.executorService.shutdown();
    }

    public boolean isAsync() {
        return async;
    }

    public boolean isPlaybackEnabled() {
        return playbackEnabled;
    }

    public String getPlaybackAttribute() {
        return playbackAttribute;
    }

    public Comparable getProperty(Comparable property) {
        return (Comparable) this.properties.get(property);
    }
}
