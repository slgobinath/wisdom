package com.javahelps.wisdom.core.event;

import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;

/**
 * {@link AttributeOperator} provides some built-in operations on the given attribute of an {@link Event} at the
 * runtime.
 * {@link AttributeOperator} modifies the attributes of the {@link Event} which is passed as the parameter of the
 * {@link Function}.
 *
 * @see com.javahelps.wisdom.core.processor.SelectProcessor
 */
public class AttributeOperator {

    protected final String name;

    public AttributeOperator(String name) {
        this.name = name;
    }

    public static AttributeOperator attribute(String attribute) {
        return new AttributeOperator(attribute);
    }

    //    public Attribute ADD(Comparable valueToAdd) {
//        Function<Event, Event> function = event -> {
//            if (event.get(name) instanceof Number && valueToAdd instanceof Number) {
//                event.set(name, ((Number) event.get(name)).doubleValue() + ((Number) valueToAdd).doubleValue());
//                return event;
//            } else {
//                throw new WisdomAppRuntimeException(
//                        String.format("%s of type %s cannot be added with %s type %s",
//                                this.name, event.get(name).getClass().getSimpleName(),
//                                valueToAdd, valueToAdd.getClass().getSimpleName()));
//            }
//        };
//        this.function = this.function.andThen(function);
//        return this;
//    }
//
//    public Attribute AS(String newName) {
//        Function<Event, Event> function = event -> {
//            if (Objects.equals(this.name, newName)) {
//                return event;
//            } else {
//                Comparable value = event.get(this.name);
//                event.remove(this.name).set(newName, value);
//                return event;
//            }
//        };
//        this.function = this.function.andThen(function);
//        return this;
//    }
//
//
    public Predicate<Event> EQUALS(Object value) {

        return event -> event.get(name).equals(value);
    }

    public Predicate<Event> GREATER_THAN(String attribute) {

        return event -> {
            System.out.println(event.getData());
            boolean result = event.get(name).compareTo(event.get(attribute)) > 0;
            return result;
        };
    }

    public Predicate<Event> GREATER_THAN(Supplier<Comparable> supplier) {

        return event -> event.get(name).compareTo(supplier.get()) > 0;
    }

    public Predicate<Event> GREATER_THAN(Number value) {

        return event -> event.get(name).compareTo(value) > 0;
    }

    public Predicate<Event> GREATER_THAN_OR_EQUAL(Number value) {

        return event -> event.get(name).compareTo(value) >= 0;
    }

    public Predicate<Event> LESS_THAN(Number value) {

        return event -> event.get(name).compareTo(value) < 0;
    }

    public Predicate<Event> LESS_THAN_OR_EQUAL(Number value) {

        return event -> event.get(name).compareTo(value) <= 0;
    }
}
