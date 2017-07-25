package com.javahelps.wisdom.core.event;

import java.util.function.Function;
import java.util.function.Predicate;

/**
 * {@link AttributePredicate} is expected to be used with {@link com.javahelps.wisdom.core.processor.FilterProcessor}.
 * Compared to {@link Attribute}, {@link AttributePredicate} does not modify the {@link Event} if used as a {@link Predicate}.
 *
 * @see com.javahelps.wisdom.core.processor.FilterProcessor
 */
public class AttributePredicate extends Attribute implements Predicate<Event> {

    public AttributePredicate(String name, Function<Event, Event> function) {
        super(name);
        this.function = function;
    }

    @Override
    public boolean test(Event event) {
        return (boolean) this.apply(event).get(this.name);
    }

}
