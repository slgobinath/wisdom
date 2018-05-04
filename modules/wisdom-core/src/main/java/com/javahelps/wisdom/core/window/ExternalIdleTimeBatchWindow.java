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
 * TimeBatchWindow expires events after a given idle period of external timestamp.
 */
@WisdomExtension("externalIdleTimeBatch")
public class ExternalIdleTimeBatchWindow extends Window implements Variable.OnUpdateListener<Number> {

    private final String timestampKey;
    private long minIdleTime;
    private List<Event> events = new ArrayList<>();
    private long lastTime = -1;
    private Variable<Number> timeVariable;

    public ExternalIdleTimeBatchWindow(Map<String, ?> properties) {
        super(properties);
        Object keyVal = this.getProperty("timestampKey", 0);
        Object durationVal = this.getProperty("duration", 1);
        if (keyVal instanceof String) {
            this.timestampKey = (String) keyVal;
        } else {
            throw new WisdomAppValidationException("timestampKey of ExternalTimeBatchWindow must be java.lang.String but found %d", keyVal.getClass().getSimpleName());
        }
        if (durationVal instanceof Number) {
            this.minIdleTime = ((Number) durationVal).longValue();
        } else if (durationVal instanceof Variable) {
            this.timeVariable = (Variable<Number>) durationVal;
            this.minIdleTime = this.timeVariable.get().longValue();
            this.timeVariable.addOnUpdateListener(this);
        } else {
            throw new WisdomAppValidationException("duration of ExternalTimeBatchWindow must be long but found %d",
                    keyVal.getClass().getSimpleName());
        }
    }

    @Override
    public void process(Event event, Processor nextProcessor) {

        List<Event> eventsToSend = null;
        long currentTimestamp = event.getAsLong(this.timestampKey);

        try {
            this.lock.lock();
            if (events.isEmpty()) {
                this.lastTime = currentTimestamp;
            }

            if (currentTimestamp - this.lastTime >= this.minIdleTime) {
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
        return new ExternalIdleTimeBatchWindow(this.properties);
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
            this.timeVariable.removeOnUpdateListener(this);
        }
        this.events = null;
    }

    @Override
    public void update(Number value) {
        try {
            this.lock.lock();
            long val = value.longValue();
            this.minIdleTime = val;
        } finally {
            this.lock.unlock();
        }
    }
}
