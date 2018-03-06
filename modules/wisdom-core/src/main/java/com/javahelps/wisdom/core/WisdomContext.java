package com.javahelps.wisdom.core;

import com.javahelps.wisdom.core.util.Scheduler;
import com.javahelps.wisdom.core.util.SystemTimestampGenerator;
import com.javahelps.wisdom.core.util.TimestampGenerator;

import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;

/**
 * The user defined and system defined configurations of the stream processor are stored in {@link WisdomContext}.
 */
public class WisdomContext {

    private TimestampGenerator timestampGenerator;
    private ScheduledExecutorService scheduledExecutorService;
    private Scheduler scheduler;
    private ExecutorService executorService;
    private ThreadFactory threadFactory;

    public WisdomContext() {
        this.timestampGenerator = new SystemTimestampGenerator();
        this.scheduledExecutorService = Executors.newScheduledThreadPool(4);
        this.scheduler = new Scheduler(this);
        this.executorService = Executors.newCachedThreadPool();
        this.threadFactory = Executors.defaultThreadFactory();
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

    public void start() {

    }

    public void shutdown() {
        this.scheduledExecutorService.shutdown();
        this.executorService.shutdown();
    }
}
