package com.javahelps.wisdom.core.event;

import com.javahelps.wisdom.core.operator.Operator;
import com.javahelps.wisdom.core.processor.AttributeSelectProcessor;

import java.util.function.Function;
import java.util.function.Supplier;

/**
 * {@link Attribute} provides some built-in operations on the given attribute of an {@link Event} at the runtime.
 * {@link Attribute} modifies the attributes of the {@link Event} which is passed attrName the parameter of the
 * {@link Function}.
 *
 * @see AttributeSelectProcessor
 */
public class Attribute extends Operator implements Supplier<Object> {

    private final String name;
    private final Supplier<Event> eventSupplier;

    public Attribute(Event event, String name) {
        this(() -> event, name);
    }

    public Attribute(Supplier<Event> eventSupplier, String name) {
        this.name = name;
        this.eventSupplier = eventSupplier;
    }

    public static Function<Event, Object> of(String name) {
        return event -> event.get(name);
    }

    @Override
    public Object get() {
        return this.eventSupplier.get().get(this.name);
    }
}
