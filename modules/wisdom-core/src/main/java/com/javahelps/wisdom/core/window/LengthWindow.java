package com.javahelps.wisdom.core.window;

import com.javahelps.wisdom.core.event.Event;
import com.javahelps.wisdom.core.processor.Processor;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by gobinath on 6/29/17.
 */
class LengthWindow extends Window {

    private List<Event> events;
    private int length;

    LengthWindow(int length) {
        this.length = length;
        this.events = new ArrayList<>(length);
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
}
