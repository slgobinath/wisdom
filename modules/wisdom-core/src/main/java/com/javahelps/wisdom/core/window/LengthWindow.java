package com.javahelps.wisdom.core.window;

import com.javahelps.wisdom.core.event.Event;
import com.javahelps.wisdom.core.processor.Processor;
import com.javahelps.wisdom.core.variable.Variable;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by gobinath on 6/29/17.
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
            synchronized (this) {
                this.length = (Integer) value;
            }
        });
    }

    public void process(Event event, Processor nextProcessor) {
        events.add(event);
        List<Event> eventsToSend = null;
        if (events.size() >= length) {
            eventsToSend = new ArrayList<>(events);
            events.clear();
        }

        if (eventsToSend != null) {
            nextProcessor.process(eventsToSend);
        }
    }

    @Override
    public Window copy() {

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
    }
}
