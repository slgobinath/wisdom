package com.javahelps.wisdom.core.util;

import com.javahelps.wisdom.core.WisdomContext;

import java.time.Duration;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * Created by gobinath on 7/10/17.
 */
public class Scheduler {

    private ScheduledExecutorService scheduledExecutorService;
    private TimestampGenerator timestampGenerator;

    public Scheduler(WisdomContext context) {
        this.scheduledExecutorService = context.getScheduledExecutorService();
        this.timestampGenerator = context.getTimestampGenerator();
    }

    public ScheduledFuture schedule(Duration duration, Executor executor) {
        return this.scheduledExecutorService.schedule(new TaskRunner(executor), duration.toMillis(), TimeUnit
                .MILLISECONDS);
    }

    private class TaskRunner implements Runnable {

        private final Executor executor;

        private TaskRunner(Executor executor) {
            this.executor = executor;
        }

        @Override
        public void run() {
            try {
                this.executor.execute(timestampGenerator.currentTimestamp());
            } catch (Exception ex) {

            }
        }
    }
}
