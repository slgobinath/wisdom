package com.javahelps.wisdom.core.stream;

import com.javahelps.wisdom.core.WisdomApp;
import com.javahelps.wisdom.core.event.Event;
import com.javahelps.wisdom.core.exception.WisdomAppRuntimeException;
import com.javahelps.wisdom.core.processor.Processor;

import java.util.ArrayList;
import java.util.List;

/**
 * {@link Stream} is the fundamental data-structure of the event processor. At the runtime, it acts as the entry point
 * and manager of all the queries starting with this as the input stream.
 */
public class Stream implements Processor {

    protected String id;
    private WisdomApp wisdomApp;
    private List<Processor> processorList = new ArrayList<>();
    private Processor[] processors;
    private int noOfProcessors;

    public Stream(String id) {
        this.id = id;
    }

    public Stream(WisdomApp wisdomApp, String id) {
        this.wisdomApp = wisdomApp;
        this.id = id;
    }

    @Override
    public void start() {
        this.noOfProcessors = this.processorList.size();
        this.processors = this.processorList.toArray(new Processor[0]);
    }

    @Override
    public void process(Event event) {
        Event newEvent = this.convertEvent(event);
        if (this.noOfProcessors == 1) {
            this.processors[0].process(newEvent);
        } else {
            for (Processor processor : this.processors) {
                try {
                    processor.process(newEvent);
                } catch (WisdomAppRuntimeException ex) {
                    this.wisdomApp.handleException(ex);
                }
            }
        }
    }

    @Override
    public void process(List<Event> events) {
        if (this.noOfProcessors == 1) {
            this.processors[0].process(events);
        } else {
            for (Processor processor : this.processors) {
                List<Event> newEvents = this.convertEvent(events);
                processor.process(newEvents);
            }
        }
    }

    public String getId() {
        return id;
    }

    private Event convertEvent(Event from) {

        Event newEvent = from.copyEvent();
        newEvent.setStream(this);
        return newEvent;
    }

    private List<Event> convertEvent(List<Event> from) {

        List<Event> newEvents = new ArrayList<>(from.size());
        for (Event event : from) {
            newEvents.add(convertEvent(event));
        }
        return newEvents;
    }

    public void addProcessor(Processor processor) {
        if (!this.processorList.contains(processor)) {
            this.processorList.add(processor);
        }
    }

    public void removeProcessor(Processor processor) {
        this.processorList.remove(processor);
    }

    @Override
    public Object clone() {

        return this;
    }
}
