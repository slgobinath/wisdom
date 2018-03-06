package com.javahelps.wisdom.core.window;

import com.javahelps.wisdom.core.event.Event;
import com.javahelps.wisdom.core.exception.WisdomAppValidationException;
import com.javahelps.wisdom.core.processor.Processor;
import com.javahelps.wisdom.core.variable.Variable;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Window keeps n number of events.
 */
public class LengthWindow extends Window implements Variable.OnUpdateListener<Integer> {

    private List<Event> events;
    private int length;

    public LengthWindow(Map<String, ?> properties) {
        super(properties);
        Object val = this.getProperty("length", 0);
        if (val instanceof Variable) {
            Variable<Integer> variable = (Variable<Integer>) val;
            this.length = variable.get();
            variable.addOnUpdateListener(this);
        } else if (val instanceof Number) {
            this.length = ((Number) val).intValue();
        } else {
            throw new WisdomAppValidationException("length of LengthWindow must be java.lang.Integer but found %s", val.getClass().getCanonicalName());
        }
        this.events = new ArrayList<>(this.length);
    }

    public void process(Event event, Processor nextProcessor) {
        events.add(event);
        List<Event> eventsToSend = null;
        try {
            this.lock.lock();
            if (events.size() >= length) {
                eventsToSend = new ArrayList<>(events);
                events.clear();
            }
        } finally {
            this.lock.unlock();
        }

        if (eventsToSend != null) {
            nextProcessor.process(eventsToSend);
        }
    }

    @Override
    public Window copy() {

        try {
            this.lock.lock();
            return new LengthWindow(this.properties);
        } finally {
            this.lock.unlock();
        }
    }

    @Override
    public void clear() {
        try {
            this.lock.lock();
            this.events.clear();
        } finally {
            this.lock.unlock();
        }
    }

    @Override
    public void update(Integer value) {
        try {
            this.lock.lock();
            this.length = value;
        } finally {
            this.lock.unlock();
        }
    }
}
