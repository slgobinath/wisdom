package com.javahelps.wisdom.core.processor;

import com.javahelps.wisdom.core.event.Event;

import java.util.Arrays;
import java.util.List;

/**
 * {@link StreamProcessor} to create a new {@link Event} with a subset of the attributes of previous {@link Event}.
 */
public class AttributeSelectProcessor extends StreamProcessor {

    private final boolean selectAll;
    private List<String> attributes;

    public AttributeSelectProcessor(String id, String... attributes) {
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
            for (Event event : events) {
                event.getData().keySet().retainAll(attributes);
            }
        }
        this.getNextProcessor().process(events);
    }

    @Override
    public Processor copy() {

        AttributeSelectProcessor attributeSelectProcessor = new AttributeSelectProcessor(this.id, this.attributes.toArray(new String[0]));
        attributeSelectProcessor.setNextProcessor(this.getNextProcessor().copy());
        return attributeSelectProcessor;
    }
}
