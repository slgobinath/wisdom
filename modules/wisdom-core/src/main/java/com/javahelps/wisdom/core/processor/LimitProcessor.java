package com.javahelps.wisdom.core.processor;

import com.javahelps.wisdom.core.event.Event;
import com.javahelps.wisdom.core.exception.WisdomAppRuntimeException;

import java.util.Arrays;
import java.util.List;

public class LimitProcessor extends StreamProcessor {

    private final int[] bounds;
    private final int maxIndex;

    public LimitProcessor(String id, int... bounds) {
        super(id);
        this.bounds = bounds;
        this.maxIndex = this.bounds.length - 1;
        Arrays.sort(this.bounds);
    }

    @Override
    public void start() {

    }

    @Override
    public void process(Event event) {
        throw new WisdomAppRuntimeException("LimitProcessor cannot be used with single event");
    }

    @Override
    public void process(List<Event> events) {
        List<Event> eventsToProcess = events;
        int noOfEvents = events.size();
        for (int i = maxIndex; i >= 0; i--) {
            if (this.bounds[i] <= noOfEvents) {
                eventsToProcess = events.subList(0, this.bounds[i]);
                break;
            }
        }
        this.getNextProcessor().process(eventsToProcess);
    }

    @Override
    public Processor copy() {
        return this;
    }

    @Override
    public void destroy() {

    }
}
