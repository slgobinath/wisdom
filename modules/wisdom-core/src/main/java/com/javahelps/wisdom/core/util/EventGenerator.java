package com.javahelps.wisdom.core.util;

import com.javahelps.wisdom.core.event.Event;
import com.javahelps.wisdom.core.exception.EventValidationException;
import com.javahelps.wisdom.core.stream.Stream;

/**
 * A utility class to create {@link Event}s from different type of inputs.
 */
public class EventGenerator {

    private EventGenerator() {

    }

    public static Event generate(Comparable... entries) {
        int count = entries.length;
        if (count % 2 != 0) {
            throw new EventValidationException("The given values must be key, value pairs with even number of " +
                    "parameters");
        } else {
            Event event = new Event(System.currentTimeMillis());
            for (int i = 0; i < count; i += 2) {
                event.set((String) entries[i], entries[i + 1]);
            }
            return event;
        }
    }

    public static Event generate(Stream stream, Comparable... entries) {
        int count = stream.getAttributes().length;
        if (count != entries.length) {
            throw new EventValidationException(String.format("Stream %s requires %d parameters but received %d " +
                    "parameters", stream.getId(), count, entries.length));
        } else {
            String[] attributes = stream.getAttributes();
            Event event = new Event(System.currentTimeMillis());
            for (int i = 0; i < count; i++) {
                event.set(attributes[i], entries[i]);
            }
            return event;
        }
    }
}
