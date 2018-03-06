package com.javahelps.wisdom.core.window;

import com.javahelps.wisdom.core.event.Event;
import com.javahelps.wisdom.core.exception.WisdomAppValidationException;
import com.javahelps.wisdom.core.processor.Processor;
import com.javahelps.wisdom.core.variable.Variable;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * TimeBatchWindow depending on external timestamp.
 */
public class ExternalTimeBatchWindow extends Window implements Variable.OnUpdateListener<Number> {

    private final String timestampKey;
    private long timeToKeep;
    private List<Event> events = new ArrayList<>();
    private long endTime = -1;

    public ExternalTimeBatchWindow(Map<String, ?> properties) {
        super(properties);
        Object keyVal = this.getProperty("timestampKey", 0);
        Object durationVal = this.getProperty("duration", 1);
        if (keyVal instanceof String) {
            this.timestampKey = (String) keyVal;
        } else {
            throw new WisdomAppValidationException("timestampKey of ExternalTimeBatchWindow must be java.lang.String but found %d", keyVal.getClass().getSimpleName());
        }
        if (durationVal instanceof Duration) {
            this.timeToKeep = ((Duration) durationVal).toMillis();
        } else if (durationVal instanceof Variable) {
            Variable<Number> variable = (Variable<Number>) durationVal;
            this.timeToKeep = variable.get().longValue();
            variable.addOnUpdateListener(this);
        } else {
            throw new WisdomAppValidationException("duration of ExternalTimeBatchWindow must be java.time.Duration but found %d", keyVal.getClass().getSimpleName());
        }
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
        try {
            this.lock.lock();
            return new ExternalTimeBatchWindow(this.properties);
        } finally {
            this.lock.unlock();
        }
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

    @Override
    public void update(Number value) {
        try {
            this.lock.lock();
            long val = value.longValue();
            this.endTime = this.endTime - this.timeToKeep + val;
            this.timeToKeep = val;
        } finally {
            this.lock.unlock();
        }
    }
}
