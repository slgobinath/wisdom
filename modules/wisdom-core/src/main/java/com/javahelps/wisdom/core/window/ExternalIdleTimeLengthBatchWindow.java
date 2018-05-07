package com.javahelps.wisdom.core.window;

import com.javahelps.wisdom.core.event.Event;
import com.javahelps.wisdom.core.exception.WisdomAppValidationException;
import com.javahelps.wisdom.core.extension.WisdomExtension;
import com.javahelps.wisdom.core.processor.Processor;
import com.javahelps.wisdom.core.variable.Variable;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * BatchWindow expires events after a given idle period of external timestamp or length.
 */
@WisdomExtension("externalIdleTimeLengthBatch")
public class ExternalIdleTimeLengthBatchWindow extends Window {

    private final String timestampKey;
    private long minIdleTime;
    private List<Event> events = new ArrayList<>();
    private long lastTime = -1;
    private Variable<Number> timeVariable;
    private int length;
    private Variable<Number> lengthVariable;
    private Variable.OnUpdateListener<Number> timeUpdater;
    private Variable.OnUpdateListener<Number> lengthUpdater;

    public ExternalIdleTimeLengthBatchWindow(Map<String, ?> properties) {
        super(properties);
        Object keyVal = this.getProperty("timestampKey", 0);
        Object durationVal = this.getProperty("duration", 1);
        Object lengthVal = this.getProperty("length", 2);
        if (keyVal instanceof String) {
            this.timestampKey = (String) keyVal;
        } else {
            throw new WisdomAppValidationException("timestampKey of ExternalIdleTimeLengthBatchWindow must be java.lang.String but found %d", keyVal.getClass().getSimpleName());
        }
        if (durationVal instanceof Number) {
            this.minIdleTime = ((Number) durationVal).longValue();
        } else if (durationVal instanceof Variable) {
            this.timeVariable = (Variable<Number>) durationVal;
            this.minIdleTime = this.timeVariable.get().longValue();
            this.timeVariable.addOnUpdateListener(this.timeUpdater);
        } else {
            throw new WisdomAppValidationException("duration of ExternalIdleTimeLengthBatchWindow must be long but found %d",
                    keyVal.getClass().getSimpleName());
        }
        if (lengthVal instanceof Variable) {
            this.lengthVariable = (Variable<Number>) lengthVal;
            this.length = this.lengthVariable.get().intValue();
            this.lengthVariable.addOnUpdateListener(this.lengthUpdater);
        } else if (lengthVal instanceof Number) {
            this.length = ((Number) lengthVal).intValue();
        } else {
            throw new WisdomAppValidationException("length of ExternalIdleTimeLengthBatchWindow must be java.lang.Integer but found %s", lengthVal.getClass().getCanonicalName());
        }
    }

    @Override
    public void process(Event event, Processor nextProcessor) {

        List<Event> eventsToSend = null;
        long currentTimestamp = event.getAsLong(this.timestampKey);

        try {
            this.lock.lock();
            int noOfEvents = events.size();
            if (noOfEvents == 0) {
                this.lastTime = currentTimestamp;
            }

            if (noOfEvents >= this.length || currentTimestamp - this.lastTime >= this.minIdleTime) {
                // Timeout happened
                eventsToSend = new ArrayList<>(this.events);
                this.events.clear();
            }
            this.events.add(event);
            this.lastTime = currentTimestamp;
        } finally {
            this.lock.unlock();
        }

        if (eventsToSend != null) {
            nextProcessor.process(eventsToSend);
        }
    }

    @Override
    public Window copy() {
        return new ExternalIdleTimeLengthBatchWindow(this.properties);
    }

    @Override
    public void clear() {
        try {
            this.lock.lock();
            this.events.clear();
            this.lastTime = -1;
        } finally {
            this.lock.unlock();
        }
    }

    @Override
    public void destroy() {
        if (this.timeVariable != null) {
            this.timeVariable.removeOnUpdateListener(this.timeUpdater);
            this.lengthVariable.removeOnUpdateListener(this.lengthUpdater);
        }
        this.events = null;
    }

    public void updateTime(Number value) {
        try {
            this.lock.lock();
            long val = value.longValue();
            this.minIdleTime = val;
        } finally {
            this.lock.unlock();
        }
    }

    public void updateLength(Number value) {
        try {
            this.lock.lock();
            int val = value.intValue();
            this.length = val;
        } finally {
            this.lock.unlock();
        }
    }
}
