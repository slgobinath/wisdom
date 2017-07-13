package com.javahelps.wisdom.core;

import com.javahelps.wisdom.core.util.Scheduler;
import com.javahelps.wisdom.core.util.SystemTimestampGenerator;
import com.javahelps.wisdom.core.util.TimestampGenerator;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

/**
 * The user defined and system defined configurations of the stream processor are stored in {@link WisdomContext}.
 */
public class WisdomContext {

    private TimestampGenerator timestampGenerator;
    private ScheduledExecutorService scheduledExecutorService;
    private Scheduler scheduler;

    public WisdomContext() {
        this.timestampGenerator = new SystemTimestampGenerator();
        this.scheduledExecutorService = Executors.newScheduledThreadPool(4);
        this.scheduler = new Scheduler(this);
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

    public void start() {

    }

    public void shutdown() {
        this.scheduledExecutorService.shutdown();
    }
}
