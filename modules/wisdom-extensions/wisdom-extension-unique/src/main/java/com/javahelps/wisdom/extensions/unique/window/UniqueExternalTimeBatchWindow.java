package com.javahelps.wisdom.extensions.unique.window;

import com.javahelps.wisdom.core.event.Event;
import com.javahelps.wisdom.core.processor.Processor;

import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class UniqueExternalTimeBatchWindow extends UniqueWindow {

    private final Map<Comparable, Event> eventMap = new LinkedHashMap<>();
    private final String uniqueKey;
    private final String timestampKey;
    private final long duration;
    private long endTime = -1;

    public UniqueExternalTimeBatchWindow(String uniqueKey, String timestampKey, Duration duration) {

        this.uniqueKey = uniqueKey;
        this.timestampKey = timestampKey;
        this.duration = duration.toMillis();
    }

    @Override
    public void process(Event event, Processor nextProcessor) {

        List<Event> eventsToSend = null;
        Comparable uniqueValue = event.get(this.uniqueKey);
        Long currentTimestamp = (Long) event.get(this.timestampKey);

        if (eventMap.isEmpty()) {
            this.endTime = currentTimestamp + this.duration;
        }

        if (currentTimestamp >= this.endTime) {
            // Timeout happened
            eventsToSend = new ArrayList<>(this.eventMap.values());
            this.eventMap.clear();
            this.endTime = this.findEndTime(currentTimestamp, this.endTime, this.duration);
        }
        this.eventMap.put(uniqueValue, event);


        if (eventsToSend != null) {
            nextProcessor.process(eventsToSend);
        }
    }

    private long findEndTime(long currentTime, long preEndTime, long timeToKeep) {
        // returns the next emission time based on system clock round time values.
        long elapsedTimeSinceLastEmit = (currentTime - preEndTime) % timeToKeep;
        return (currentTime + (timeToKeep - elapsedTimeSinceLastEmit));
    }
}
