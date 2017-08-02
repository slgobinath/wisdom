package com.javahelps.wisdom.core.processor;

import com.javahelps.wisdom.core.event.Event;

import java.util.List;

public class PartitionProcessor extends StreamProcessor {

    private final String[] attributes;

    public PartitionProcessor(String id, String... attributes) {
        super(id);
        this.attributes = attributes;
    }

    @Override
    public void start() {

    }

    @Override
    public void process(Event event) {
        this.getNextProcessor().process(event);
    }

    @Override
    public void process(List<Event> events) {

    }

    @Override
    public Object clone() {
        return null;
    }
}
