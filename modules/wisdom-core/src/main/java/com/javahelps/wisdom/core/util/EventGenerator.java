package com.javahelps.wisdom.core.util;

import com.javahelps.wisdom.core.event.Event;
import com.javahelps.wisdom.core.exception.EventValidationException;

import java.util.Map;

/**
 * A utility class to create {@link Event}s from different type of inputs.
 */
public class EventGenerator {

    private static final Event RESET_EVENT = new Event(-1L);

    static {
        RESET_EVENT.setReset(true);
    }

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

    public static Event generate(Map<String, Object> map) {
        Event event = new Event(System.currentTimeMillis());
        event.getData().putAll(map);
        return event;
    }

    public static Event getResetEvent() {
        return RESET_EVENT;
    }
}
