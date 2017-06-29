package com.javahelps.wisdom.core.processor;

import com.javahelps.wisdom.core.event.Event;

import java.util.Collection;

/**
 * The abstract representation of all kind of {@link Event} processors.
 */
public interface Processor {

    void process(Event event);

    void process(Collection<Event> events);
}
