package com.javahelps.wisdom.core.event;

import com.javahelps.wisdom.core.exception.WisdomAppRuntimeException;

import java.util.Objects;
import java.util.function.Function;

/**
 * {@link Attribute} provides some built-in operations on the given attribute of an {@link Event} at the runtime.
 * {@link Attribute} modifies the attributes of the {@link Event} which is passed as the parameter of the
 * {@link Function}.
 *
 * @see com.javahelps.wisdom.core.processor.SelectProcessor
 */
public class Attribute implements Function<Event, Event> {

    protected String name;
    protected Function<Event, Event> function = Function.identity();

    public Attribute(String name) {
        this.name = name;
    }

    @Override
    public Event apply(Event event) {
        return this.function.apply(event);
    }

    public Attribute ADD(Comparable valueToAdd) {
        Function<Event, Event> function = event -> {
            if (event.get(name) instanceof Number && valueToAdd instanceof Number) {
                event.set(name, ((Number) event.get(name)).doubleValue() + ((Number) valueToAdd).doubleValue());
                return event;
            } else {
                throw new WisdomAppRuntimeException(
                        String.format("%s of type %s cannot be added with %s type %s",
                                this.name, event.get(name).getClass().getSimpleName(),
                                valueToAdd, valueToAdd.getClass().getSimpleName()));
            }
        };
        this.function = this.function.andThen(function);
        return this;
    }

    public Attribute AS(String newName) {
        Function<Event, Event> function = event -> {
            if (Objects.equals(this.name, newName)) {
                return event;
            } else {
                Comparable value = event.get(this.name);
                event.remove(this.name).set(newName, value);
                return event;
            }
        };
        this.function = this.function.andThen(function);
        return this;
    }


    public AttributePredicate EQUAL_TO(Object value) {

        this.function = this.function.andThen(event -> {
            event.set(this.name, Objects.equals(event.get(name), value));
            return event;
        });
        return new AttributePredicate(this.name, this.function);
    }


    public AttributePredicate GREATER_THAN(Object value) {

        this.function = this.function.andThen(event -> {
            event.set(this.name, event.get(name).compareTo(value) > 0);
            return event;
        });
        return new AttributePredicate(this.name, this.function);
    }

    public AttributePredicate GREATER_THAN_OR_EQUAL(Object value) {

        this.function = this.function.andThen(event -> {
            event.set(this.name, event.get(name).compareTo(value) >= 0);
            return event;
        });
        return new AttributePredicate(this.name, this.function);
    }

    public AttributePredicate LESS_THAN(Object value) {

        this.function = this.function.andThen(event -> {
            event.set(this.name, event.get(name).compareTo(value) < 0);
            return event;
        });
        return new AttributePredicate(this.name, this.function);
    }

    public AttributePredicate LESS_THAN_OR_EQUAL(Object value) {

        this.function = this.function.andThen(event -> {
            event.set(this.name, event.get(name).compareTo(value) <= 0);
            return event;
        });
        return new AttributePredicate(this.name, this.function);
    }
}
