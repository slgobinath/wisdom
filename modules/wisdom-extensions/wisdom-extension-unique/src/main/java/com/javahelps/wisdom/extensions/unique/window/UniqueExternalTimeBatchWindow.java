package com.javahelps.wisdom.extensions.unique.window;

import com.javahelps.wisdom.core.event.Event;
import com.javahelps.wisdom.core.processor.Processor;
import com.javahelps.wisdom.core.window.Window;

import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class UniqueExternalTimeBatchWindow extends UniqueWindow {

    private final Map<Comparable, Event> eventMap = new LinkedHashMap<>();
    private final String uniqueKey;
    private final String timestampKey;
    private final Duration duration;
    private final long timeToKeep;
    private long endTime = -1;

    public UniqueExternalTimeBatchWindow(String uniqueKey, String timestampKey, Duration duration) {

        this.uniqueKey = uniqueKey;
        this.timestampKey = timestampKey;
        this.duration = duration;
        this.timeToKeep = duration.toMillis();
    }

    @Override
    public void process(Event event, Processor nextProcessor) {

        List<Event> eventsToSend = null;
        Comparable uniqueValue = event.get(this.uniqueKey);
        long currentTimestamp = event.getAsLong(this.timestampKey);

        try {
            this.lock.lock();
            if (eventMap.isEmpty()) {
                this.endTime = currentTimestamp + this.timeToKeep;
            }

            if (currentTimestamp >= this.endTime) {
                // Timeout happened
                eventsToSend = new ArrayList<>(this.eventMap.values());
                this.eventMap.clear();
                this.endTime = this.findEndTime(currentTimestamp, this.endTime, this.timeToKeep);
            }
            this.eventMap.put(uniqueValue, event);
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

        Window window = new UniqueExternalTimeBatchWindow(this.uniqueKey, this.timestampKey, this.duration);
        return window;
    }

    @Override
    public void clear() {
        try {
            this.lock.lock();
            this.eventMap.clear();
            this.endTime = -1;
        } finally {
            this.lock.unlock();
        }
    }
}
