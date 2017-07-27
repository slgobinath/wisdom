package com.javahelps.wisdom.core.processor;

import com.javahelps.wisdom.core.event.Event;
import com.javahelps.wisdom.core.stream.Stream;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

/**
 * {@link StreamProcessor} to create a new {@link Event} with a subset of the attributes of previous {@link Event}.
 */
public class SelectProcessor extends StreamProcessor {

    private List<String> attributes;
    private final boolean selectAll;

    public SelectProcessor(String id, Stream inputStream, String... attributes) {
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
    public void process(Collection<Event> events) {
        if (!selectAll) {
            // Do nothing
            for(Event event : events) {
                event.getData().keySet().retainAll(attributes);
            }
        }
        this.getNextProcessor().process(events);
    }
}
