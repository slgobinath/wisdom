package com.javahelps.wisdom.core.processor;

import com.javahelps.wisdom.core.event.Event;
import com.javahelps.wisdom.core.exception.WisdomAppRuntimeException;
import com.javahelps.wisdom.core.stream.Stream;

import java.util.ArrayList;
import java.util.Collection;

/**
 * {@link StreamProcessor} to create a new {@link Event} with a subset of the attributes of previous {@link Event}.
 */
public class SelectProcessor extends StreamProcessor {

    private String[] attributes;
    private final boolean selectAll;

    public SelectProcessor(String id, Stream inputStream, String... attributes) {
        super(id, inputStream);
        this.selectAll = attributes.length == 0;
        this.attributes = attributes;
    }


    @Override
    public void process(Event event) {
        Event output;
        if (selectAll) {
            output = event;
        } else {
            output = new Event(event.getStream(), event.getTimestamp());
            for (String attribute : this.attributes) {
                Comparable value = event.get(attribute);
                if (value != null) {
                    output.set(attribute, value);
                } else {
                    throw new WisdomAppRuntimeException(String.format("Attribute %s does not exist in stream %s",
                            attribute, event.getStream().getId()));
                }
            }
        }
        this.getNextProcessor().process(output);
    }

    @Override
    public void process(Collection<Event> events) {
        if (!selectAll) {
            // Do nothing
            Collection<Event> eventsAfterSelection = new ArrayList<>(events.size());
            for (Event event : events) {
                Event output = new Event(event.getStream(), event.getTimestamp());
                for (String attribute : this.attributes) {
                    output.set(attribute, event.get(attribute));
                }
                eventsAfterSelection.add(output);
            }
            events = eventsAfterSelection;
        }
        this.getNextProcessor().process(events);
    }
}
