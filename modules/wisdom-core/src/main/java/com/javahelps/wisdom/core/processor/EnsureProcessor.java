package com.javahelps.wisdom.core.processor;

import com.javahelps.wisdom.core.event.Event;
import com.javahelps.wisdom.core.exception.WisdomAppRuntimeException;

import java.util.Arrays;
import java.util.List;

public class EnsureProcessor extends StreamProcessor {

    private final int[] bounds;

    public EnsureProcessor(String id, int... bounds) {
        super(id);
        this.bounds = bounds;
        Arrays.sort(this.bounds);
    }

    @Override
    public void start() {

    }

    @Override
    public void process(Event event) {
        throw new WisdomAppRuntimeException("EnsureProcessor cannot be used with single event");
    }

    @Override
    public void process(List<Event> events) {
        int noOfEvents = events.size();
        for (int bound : this.bounds) {
            if (noOfEvents <= bound) {
                int newEvents = bound - noOfEvents;
                for (int i = 0; i < newEvents; i++) {
                    events.add(events.get(0).emptyEvent());
                }
                break;
            }
        }
        this.getNextProcessor().process(events);
    }

    @Override
    public Processor copy() {
        return this;
    }

    @Override
    public void destroy() {

    }
}
