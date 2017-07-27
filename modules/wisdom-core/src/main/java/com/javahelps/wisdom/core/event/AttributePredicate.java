package com.javahelps.wisdom.core.event;

import java.util.function.Function;
import java.util.function.Predicate;

/**
 * {@link AttributePredicate} is expected to be used with {@link com.javahelps.wisdom.core.processor.FilterProcessor}.
 * Compared to {@link Attribute}, {@link AttributePredicate} does not modify the {@link Event} if used as a {@link Predicate}.
 *
 * @see com.javahelps.wisdom.core.processor.FilterProcessor
 */
public class AttributePredicate implements Predicate<Event> {

    private final Attribute attribute;


    public AttributePredicate(Attribute attribute, Function<Event, Event> function) {
        this.attribute = attribute;
    }

    @Override
    public boolean test(Event event) {
        return false;
    }

}
