package com.javahelps.wisdom.core.operator;

import com.javahelps.wisdom.core.event.Event;

import java.util.List;
import java.util.function.Function;

/**
 * {@link EventOperator} provides some built-in operations on the given attribute of an {@link Event} at the
 * runtime.
 * {@link EventOperator} modifies the attributes of the {@link Event} which is passed as the parameter of the
 * {@link Function}.
 *
 * @see com.javahelps.wisdom.core.processor.SelectProcessor
 */
public class EventOperator {

    public static final Function<List<Event>, Event> COUNT_AS(String newName) {

        Function<List<Event>, Event> function = events -> {
            long count = events.stream().count();
            Event lastEvent = events.get(events.size() - 1);
            lastEvent.set(newName, count);
            return lastEvent;
        };
        return function;
    }
}
