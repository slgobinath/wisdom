package com.javahelps.wisdom.core.processor;

import com.javahelps.wisdom.core.event.Event;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;

/**
 * {@link StreamProcessor} to create a new {@link Event} with a subset of the attributes of previous {@link Event}.
 */
public class SelectProcessor extends StreamProcessor {

    private final boolean selectAll;
    private List<String> attributes;

    public SelectProcessor(String id, String... attributes) {
        super(id);
        this.selectAll = attributes.length == 0;
        this.attributes = Arrays.asList(attributes);
    }

    public void init() {

    }

    @Override
    public void start() {

    }


    @Override
    public void process(Event event) {
        if (!selectAll) {
            event.getData().keySet().retainAll(attributes);
        }
        this.getNextProcessor().process(event);
    }

    @Override
    public void process(List<Event> events) {
        if (!selectAll) {
            // Do nothing
            for (Event event : events) {
                event.getData().keySet().retainAll(attributes);
            }
        }
        this.getNextProcessor().process(events);
    }

    @Override
    public Object clone() {

        SelectProcessor selectProcessor = new SelectProcessor(this.id, this.attributes.toArray(new String[0]));
        selectProcessor.setNextProcessor((Processor) this.getNextProcessor().clone());
        return selectProcessor;
    }
}
