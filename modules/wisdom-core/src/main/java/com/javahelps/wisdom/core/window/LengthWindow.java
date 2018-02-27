package com.javahelps.wisdom.core.window;

import com.javahelps.wisdom.core.event.Event;
import com.javahelps.wisdom.core.processor.Processor;
import com.javahelps.wisdom.core.variable.Variable;

import java.util.ArrayList;
import java.util.List;

/**
 * Window keeps n number of events.
 */
class LengthWindow extends Window {

    private List<Event> events;
    private int length;
    private Variable<Integer> variable;

    LengthWindow(int length) {
        this.length = length;
        this.events = new ArrayList<>(length);
    }

    LengthWindow(Variable<Integer> length) {
        this(length.get());
        length.addOnUpdateListener(value -> {
            try {
                this.lock.lock();
                this.length = (Integer) value;
            } finally {
                this.lock.unlock();
            }
        });
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
            LengthWindow window = new LengthWindow(this.length);
            if (this.variable != null) {
                window.variable = this.variable;
                variable.addOnUpdateListener(value -> {
                    synchronized (window) {
                        window.length = (Integer) value;
                    }
                });
            }
            return window;
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
}
