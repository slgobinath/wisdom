package com.javahelps.wisdom.core.time;

import java.time.Duration;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class SystemScheduler implements Scheduler {

    private final ScheduledExecutorService scheduledExecutorService;
    private final TimestampGenerator timestampGenerator;

    public SystemScheduler(ScheduledExecutorService scheduledExecutorService, TimestampGenerator timestampGenerator) {
        this.scheduledExecutorService = scheduledExecutorService;
        this.timestampGenerator = timestampGenerator;
    }

    @Override
    public void schedule(Duration duration, Executor executor) {
        this.scheduledExecutorService.schedule(new TaskRunner(executor), duration.toMillis(), TimeUnit.MILLISECONDS);
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
