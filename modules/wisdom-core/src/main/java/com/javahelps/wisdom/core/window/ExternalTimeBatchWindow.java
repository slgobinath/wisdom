package com.javahelps.wisdom.core.window;

import com.javahelps.wisdom.core.event.Event;
import com.javahelps.wisdom.core.processor.Processor;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by gobinath on 6/29/17.
 */
class ExternalTimeBatchWindow extends Window {

    private List<Event> events;
    private final String timestampKey;
    private final Duration duration;
    private final long timeToKeep;
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
}
