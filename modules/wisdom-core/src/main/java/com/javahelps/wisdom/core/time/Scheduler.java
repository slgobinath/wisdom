package com.javahelps.wisdom.core.time;

import java.time.Duration;

/**
 * Wisdom scheduler for scheduling tasks to run later.
 */
public interface Scheduler {

    void schedule(Duration duration, Executor executor);
}
