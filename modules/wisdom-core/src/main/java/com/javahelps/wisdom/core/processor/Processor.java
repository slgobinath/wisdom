package com.javahelps.wisdom.core.processor;

import com.javahelps.wisdom.core.event.Event;
import com.javahelps.wisdom.core.partition.Partitionable;

import java.io.Serializable;
import java.util.List;

/**
 * The abstract representation of all kind of {@link Event} processors.
 */
public interface Processor extends Partitionable, Serializable {

    void start();

    void stop();

    void process(Event event);

    void process(List<Event> events);

    Processor copy();

}
