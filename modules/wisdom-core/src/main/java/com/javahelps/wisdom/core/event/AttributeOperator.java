package com.javahelps.wisdom.core.event;

import com.javahelps.wisdom.core.util.WisdomConfig;

import java.util.Comparator;
import java.util.List;
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

    public Function<List<Event>, Event> SUM_AS(String newName) {

        Function<List<Event>, Event> function = events -> {
            double sum = events.stream().mapToDouble(e -> ((Number) e.get(this.name)).doubleValue()).sum();
            Event lastEvent = events.get(events.size() - 1);
            lastEvent.set(newName, sum);
            return lastEvent;
        };
        return function;
    }

    public Function<List<Event>, Event> MIN_AS(String newName) {

        Function<List<Event>, Event> function = events -> {
            Object min = events.stream().map(event -> event.get(this.name)).min(Comparator.naturalOrder()).get();
            Event lastEvent = events.get(events.size() - 1);
            lastEvent.set(newName, (Comparable) min);
            return lastEvent;
        };
        return function;
    }

    public Function<List<Event>, Event> MAX_AS(String newName) {

        Function<List<Event>, Event> function = events -> {
            Object min = events.stream().map(event -> event.get(this.name)).max(Comparator.naturalOrder()).get();
            Event lastEvent = events.get(events.size() - 1);
            lastEvent.set(newName, (Comparable) min);
            return lastEvent;
        };
        return function;
    }

    public Function<List<Event>, Event> AVG_AS(String newName) {

        Function<List<Event>, Event> function = events -> {
            double avg = events.stream().mapToDouble(e -> ((Number) e.get(this.name)).doubleValue()).average().orElse
                    (Double.NaN);
            if (avg != Double.NaN) {
                avg = Math.round(avg * WisdomConfig.DOUBLE_PRECISION) / WisdomConfig.DOUBLE_PRECISION;
            }
            Event lastEvent = events.get(events.size() - 1);
            lastEvent.set(newName, avg);
            return lastEvent;
        };
        return function;
    }

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
