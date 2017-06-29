package com.javahelps.wisdom.core.stream;

import com.javahelps.wisdom.core.WisdomApp;
import com.javahelps.wisdom.core.event.Event;
import com.javahelps.wisdom.core.exception.WisdomAppRuntimeException;
import com.javahelps.wisdom.core.processor.Processor;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * {@link Stream} is the fundamental data-structure of the event processor. At the runtime, it acts as the entry point
 * and manager of all the queries starting with this as the input stream.
 */
public class Stream implements Processor {

    protected String id;
    protected String[] attributes;
    private List<String> attributesList;
    private WisdomApp wisdomApp;
    private Set<Processor> processors = new HashSet<>();

    public Stream(String id) {
        this.id = id;
    }

    public Stream(WisdomApp wisdomApp, String id, String... attributes) {
        this.wisdomApp = wisdomApp;
        this.id = id;
        this.attributes = attributes;
        this.attributesList = Arrays.asList(attributes);
    }

    @Override
    public void process(Event event) {
        for (Processor processor : this.processors) {
            Event newEvent = this.convertEvent(event);
            try {
                processor.process(newEvent);
            } catch (WisdomAppRuntimeException ex) {
                this.wisdomApp.handleException(ex);
            }
        }
    }

    @Override
    public void process(Collection<Event> events) {
        for (Processor processor : this.processors) {
            Collection<Event> newEvents = this.convertEvent(events);
            processor.process(newEvents);
        }
    }

    public String getId() {
        return id;
    }

    public String[] getAttributes() {
        return attributes;
    }

    private Event convertEvent(Event from) {

        Event newEvent = new Event(this, from.getTimestamp());
        for (String attribute : this.attributes) {
            newEvent.set(attribute, from.get(attribute));
        }
        return newEvent;
    }

    private Collection<Event> convertEvent(Collection<Event> from) {

        Collection<Event> newEvents = new ArrayList<>(from.size());
        for (Event event : from) {
            newEvents.add(convertEvent(event));
        }
        return newEvents;
    }

    public void addProcessor(Processor processor) {
        this.processors.add(processor);
    }

    public boolean contains(String attribute) {
        return this.attributesList.contains(attribute);
    }
}
