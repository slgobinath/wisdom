package com.javahelps.wisdom.core.processor;

import com.javahelps.wisdom.core.event.Event;
import com.javahelps.wisdom.core.event.Index;
import com.javahelps.wisdom.core.exception.WisdomAppRuntimeException;

import java.util.List;

/**
 * {@link StreamProcessor} to create a new {@link Event} with a subset of the attributes of previous {@link Event}.
 */
public class EventSelectProcessor extends StreamProcessor {

    private Index index;

    public EventSelectProcessor(String id, Index index) {
        super(id);
        this.index = index;
    }

    public void init() {

    }

    @Override
    public void start() {

    }


    @Override
    public void process(Event event) {
        throw new WisdomAppRuntimeException("EventSelector cannot be used with single event stream.");
    }

    @Override
    public void process(List<Event> events) {
        int length = events.size();
        if (length > 0) {
            if (this.index == Event.FIRST) {
                this.getNextProcessor().process(events.get(0));
            } else if (this.index == Event.LAST) {
                this.getNextProcessor().process(events.get(length - 1));
            }
        }
    }

    @Override
    public Processor copy() {

        EventSelectProcessor eventSelectProcessor = new EventSelectProcessor(this.id, this.index);
        eventSelectProcessor.setNextProcessor(this.getNextProcessor().copy());
        return eventSelectProcessor;
    }
}
