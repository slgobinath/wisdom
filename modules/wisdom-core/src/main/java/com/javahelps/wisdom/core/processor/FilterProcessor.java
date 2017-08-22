package com.javahelps.wisdom.core.processor;

import com.javahelps.wisdom.core.event.Event;

import java.util.List;
import java.util.function.Predicate;

/**
 * {@link StreamProcessor} to filter the events based on a given {@link Predicate}.
 */
public class FilterProcessor extends StreamProcessor {

    private Predicate<Event> predicate;

    public FilterProcessor(String id, Predicate<Event> predicate) {
        super(id);
        this.predicate = predicate;
    }

    @Override
    public void start() {

    }

    @Override
    public void process(Event event) {
        if (this.predicate.test(event)) {
            this.getNextProcessor().process(event);
        }
    }

    @Override
    public void process(List<Event> events) {
        events.removeIf(this.predicate.negate());
        if (!events.isEmpty()) {
            this.getNextProcessor().process(events);
        }
    }

    @Override
    public Processor copy() {

        FilterProcessor filterProcessor = new FilterProcessor(this.id, this.predicate);
        filterProcessor.setNextProcessor(this.getNextProcessor().copy());
        return filterProcessor;
    }
}
