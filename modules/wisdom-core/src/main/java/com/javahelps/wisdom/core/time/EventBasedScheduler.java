package com.javahelps.wisdom.core.time;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.ExecutorService;

public class EventBasedScheduler implements Scheduler, EventBasedTimestampGenerator.TimeChangeListener {

    private final ExecutorService executorService;
    private final EventBasedTimestampGenerator timestampGenerator;
    private Map<Long, Executor> executorMap = new HashMap<>();
    private long nextSchedule = Long.MAX_VALUE;

    public EventBasedScheduler(ExecutorService executorService, EventBasedTimestampGenerator timestampGenerator) {
        this.executorService = executorService;
        this.timestampGenerator = timestampGenerator;
        this.timestampGenerator.addTimeChangeListener(this);
    }

    public void schedule(Duration duration, Executor executor) {
        if (duration.isZero() || duration.isNegative()) {
            this.executorService.submit(new TaskRunner(executor));
        } else {
            synchronized (this) {
                long timeToNotify = duration.toMillis() + timestampGenerator.currentTimestamp();
                if (timeToNotify < this.nextSchedule) {
                    this.nextSchedule = timeToNotify;
                }
                this.executorMap.put(timeToNotify, executor);
            }
        }
    }

    @Override
    public void onTimeChange(long timestamp) {
        List<Executor> executors;
        synchronized (this) {
            if (timestamp < this.nextSchedule) {
                return;
            }
            executors = new ArrayList<>();
            Iterator<Map.Entry<Long, Executor>> iterator = this.executorMap.entrySet().iterator();
            while (iterator.hasNext()) {
                Map.Entry<Long, Executor> entry = iterator.next();
                long schduledTime = entry.getKey().longValue();
                if (schduledTime <= timestamp) {
                    executors.add(entry.getValue());
                    iterator.remove();
                } else {
                    this.nextSchedule = schduledTime;
                }
            }
        }
        if (executors != null) {
            for (Executor executor : executors) {
                try {
                    executor.execute(timestamp);
                } catch (Exception ex) {

                }
            }
        }
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
