package com.javahelps.wisdom.core.operator;

import com.javahelps.wisdom.core.event.Event;
import com.javahelps.wisdom.core.operand.WisdomLong;
import com.javahelps.wisdom.core.processor.AttributeSelectProcessor;
import com.javahelps.wisdom.core.util.WisdomConfig;

import java.util.function.Function;

/**
 * {@link EventOperator} provides some built-in operations on the given attribute of an {@link Event} at the
 * runtime.
 * {@link EventOperator} modifies the attributes of the {@link Event} which is passed as the parameter of the
 * {@link Function}.
 *
 * @see AttributeSelectProcessor
 */
public class EventOperator {

    public static final Function<Event, Event> COUNT_AS(String newName) {

        final WisdomLong count = new WisdomLong();
        Function<Event, Event> function;
        if (WisdomConfig.ASYNC_ENABLED) {
            function = event -> {
                synchronized (count) {
                    if (event.isReset()) {
                        count.set(0);
                    } else {
                        event.set(newName, count.incrementAndGet());
                    }
                }
                return event;
            };
        } else {
            function = event -> {
                if (event.isReset()) {
                    count.set(0);
                } else {
                    event.set(newName, count.incrementAndGet());
                }
                return event;
            };
        }
        return function;
    }
}
