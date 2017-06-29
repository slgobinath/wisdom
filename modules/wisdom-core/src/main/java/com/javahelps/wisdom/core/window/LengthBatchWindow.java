package com.javahelps.wisdom.core.window;

import com.javahelps.wisdom.core.event.Event;
import com.javahelps.wisdom.core.processor.Processor;

import java.util.ArrayList;
import java.util.Collection;

/**
 * Created by gobinath on 6/29/17.
 */
class LengthBatchWindow extends Window {

    private int length;

    LengthBatchWindow(int length) {
        this.length = length;
    }

    public void process(Collection<Event> events, Event event, Processor nextProcessor) {
        events.add(event);
        Collection<Event> eventsToSend = null;
        if (events.size() >= length) {
            eventsToSend = new ArrayList<>(events);
            events.clear();
        }

        if (eventsToSend != null) {
            nextProcessor.process(eventsToSend);
        }
    }
}
