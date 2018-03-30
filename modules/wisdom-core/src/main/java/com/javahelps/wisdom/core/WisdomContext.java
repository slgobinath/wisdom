package com.javahelps.wisdom.core;

import com.javahelps.wisdom.core.util.Scheduler;
import com.javahelps.wisdom.core.util.SystemTimestampGenerator;
import com.javahelps.wisdom.core.util.TimestampGenerator;
import com.javahelps.wisdom.core.util.WisdomConfig;

import java.util.Properties;
import java.util.concurrent.*;

import static com.javahelps.wisdom.core.util.WisdomConstants.ASYNC;

/**
 * The user defined and system defined configurations of the stream processor are stored in {@link WisdomContext}.
 */
public class WisdomContext {

    private final boolean async;
    private final Scheduler scheduler;
    private final Properties properties;
    private final ExecutorService executorService;
    private final ThreadFactory threadFactory;
    private final ThreadBarrier threadBarrier;
    private final TimestampGenerator timestampGenerator;
    private final ScheduledExecutorService scheduledExecutorService;

    public WisdomContext(Properties properties) {
        this.properties = properties;
        this.timestampGenerator = new SystemTimestampGenerator();
        this.scheduledExecutorService = Executors.newScheduledThreadPool(4);
        this.scheduler = new Scheduler(this);
        this.executorService = Executors.newCachedThreadPool();
        this.threadFactory = Executors.defaultThreadFactory();
        this.threadBarrier = new ThreadBarrier();
        this.async = (boolean) properties.getOrDefault(ASYNC, WisdomConfig.ASYNC_ENABLED);
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

    public void start() {

    }

    public void shutdown() {
        this.scheduledExecutorService.shutdown();
        this.executorService.shutdown();
    }

    public boolean isAsync() {
        return async;
    }

    public Comparable getProperty(String property) {
        return (Comparable) this.properties.get(property);
    }
}
