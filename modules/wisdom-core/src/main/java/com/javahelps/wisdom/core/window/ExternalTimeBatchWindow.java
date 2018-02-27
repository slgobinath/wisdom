package com.javahelps.wisdom.core.window;

import com.javahelps.wisdom.core.event.Event;
import com.javahelps.wisdom.core.processor.Processor;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

/**
 * TimeBatchWindow depending on external timestamp.
 */
class ExternalTimeBatchWindow extends Window {

    private final String timestampKey;
    private final Duration duration;
    private final long timeToKeep;
    private List<Event> events;
    private long endTime = -1;

    ExternalTimeBatchWindow(String timestampKey, Duration duration) {

        this.events = new ArrayList<>();
        this.timestampKey = timestampKey;
        this.duration = duration;
        this.timeToKeep = duration.toMillis();
    }

    @Override
    public void process(Event event, Processor nextProcessor) {

        List<Event> eventsToSend = null;
        long currentTimestamp = event.getAsLong(this.timestampKey);

        try {
            this.lock.lock();
            if (events.isEmpty()) {
                this.endTime = currentTimestamp + this.timeToKeep;
            }

            if (currentTimestamp >= this.endTime) {
                // Timeout happened
                eventsToSend = new ArrayList<>(this.events);
                this.events.clear();
                this.endTime = this.findEndTime(currentTimestamp, this.endTime, this.timeToKeep);
            }
            this.events.add(event);
        } finally {
            this.lock.unlock();
        }

        if (eventsToSend != null) {
            nextProcessor.process(eventsToSend);
        }
    }

    private long findEndTime(long currentTime, long preEndTime, long timeToKeep) {
        // returns the next emission time based on system clock round time values.
        long elapsedTimeSinceLastEmit = (currentTime - preEndTime) % timeToKeep;
        return (currentTime + (timeToKeep - elapsedTimeSinceLastEmit));
    }

    @Override
    public Window copy() {

        Window window = new ExternalTimeBatchWindow(this.timestampKey, this.duration);
        return window;
    }

    @Override
    public void clear() {
        try {
            this.lock.lock();
            this.events.clear();
            this.endTime = -1;
        } finally {
            this.lock.unlock();
        }
    }
}
