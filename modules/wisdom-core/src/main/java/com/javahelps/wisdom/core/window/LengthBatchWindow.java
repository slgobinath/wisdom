package com.javahelps.wisdom.core.window;

import com.javahelps.wisdom.core.event.Event;
import com.javahelps.wisdom.core.processor.Processor;
import com.javahelps.wisdom.core.variable.Variable;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by gobinath on 6/29/17.
 */
class LengthBatchWindow extends Window implements Variable.OnUpdateListener<Integer> {

    private List<Event> events;
    private int length;
    private Variable<Integer> variable;

    LengthBatchWindow(int length) {
        this.length = length;
        this.events = new ArrayList<>(length);
    }

    LengthBatchWindow(Variable<Integer> length) {
        this(length.get());
        length.addOnUpdateListener(this);
    }

    public void process(Event event, Processor nextProcessor) {
        List<Event> eventsToSend = null;
        synchronized (this) {
            events.add(event);
            if (events.size() >= length) {
                eventsToSend = new ArrayList<>(events);
                events.clear();
            }
        }
        if (eventsToSend != null) {
            nextProcessor.process(eventsToSend);
        }
    }

    @Override
    public Window copy() {

        LengthBatchWindow window = new LengthBatchWindow(this.length);
        if (this.variable != null) {
            window.variable = this.variable;
            variable.addOnUpdateListener(window);
        }
        return window;
    }

    @Override
    public void update(Integer value) {
        synchronized (this) {
            this.length = value;
        }
    }
}
