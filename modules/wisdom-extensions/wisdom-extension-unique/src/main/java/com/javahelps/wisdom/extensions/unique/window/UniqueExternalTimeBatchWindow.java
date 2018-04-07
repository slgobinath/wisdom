package com.javahelps.wisdom.extensions.unique.window;

import com.javahelps.wisdom.core.event.Event;
import com.javahelps.wisdom.core.exception.WisdomAppValidationException;
import com.javahelps.wisdom.core.extension.WisdomExtension;
import com.javahelps.wisdom.core.processor.Processor;
import com.javahelps.wisdom.core.variable.Variable;
import com.javahelps.wisdom.core.window.Window;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@WisdomExtension("unique:externalTimeBatch")
public class UniqueExternalTimeBatchWindow extends Window implements Variable.OnUpdateListener<Long> {

    private Map<Comparable, Event> eventMap = new LinkedHashMap<>();
    private final String uniqueKey;
    private final String timestampKey;
    private long timeToKeep;
    private Variable<Number> timeToKeepVariable;
    private long endTime = -1;


    public UniqueExternalTimeBatchWindow(Map<String, ?> properties) {
        super(properties);
        Object uniqueKeyVal = this.getProperty("uniqueKey", 0);
        Object timestampKeyVal = this.getProperty("timestampKey", 1);
        Object durationVal = this.getProperty("duration", 2);

        if (uniqueKeyVal instanceof String) {
            this.uniqueKey = (String) uniqueKeyVal;
        } else {
            throw new WisdomAppValidationException("uniqueKey of UniqueExternalTimeBatchWindow must be java.lang.String but found %d", timestampKeyVal.getClass().getSimpleName());
        }
        if (timestampKeyVal instanceof String) {
            this.timestampKey = (String) timestampKeyVal;
        } else {
            throw new WisdomAppValidationException("timestampKey of UniqueExternalTimeBatchWindow must be java.lang.String but found %d", timestampKeyVal.getClass().getSimpleName());
        }
        if (durationVal instanceof Number) {
            this.timeToKeep = ((Number) durationVal).longValue();
        } else if (durationVal instanceof Variable) {
            this.timeToKeepVariable = (Variable<Number>) durationVal;
            this.timeToKeep = this.timeToKeepVariable.get().longValue();
            this.timeToKeepVariable.addOnUpdateListener(this);
        } else {
            throw new WisdomAppValidationException("duration of UniqueExternalTimeBatchWindow must be long but found %d", timestampKeyVal.getClass().getSimpleName());
        }
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

        return new UniqueExternalTimeBatchWindow(this.properties);
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

    @Override
    public void destroy() {
        if (this.timeToKeepVariable != null) {
            this.timeToKeepVariable.removeOnUpdateListener(this);
        }
        this.eventMap = null;
    }

    @Override
    public void update(Long value) {
        try {
            this.lock.lock();
            this.endTime = this.endTime - this.timeToKeep + value;
            this.timeToKeep = value;
        } finally {
            this.lock.unlock();
        }
    }
}
