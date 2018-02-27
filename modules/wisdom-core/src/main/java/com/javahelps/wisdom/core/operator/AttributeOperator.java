package com.javahelps.wisdom.core.operator;

import com.javahelps.wisdom.core.event.Event;
import com.javahelps.wisdom.core.operand.WisdomDouble;
import com.javahelps.wisdom.core.operand.WisdomLong;
import com.javahelps.wisdom.core.operand.WisdomReference;
import com.javahelps.wisdom.core.processor.AttributeSelectProcessor;
import com.javahelps.wisdom.core.util.WisdomConfig;

import java.util.Comparator;
import java.util.Objects;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * {@link AttributeOperator} provides some built-in operations on the given attribute of an {@link Event} at the
 * runtime.
 * {@link AttributeOperator} modifies the attributes of the {@link Event} which is passed as the parameter of the
 * {@link Function}.
 *
 * @see AttributeSelectProcessor
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

    public Function<Event, Event> SUM_AS(String newName) {

        final WisdomDouble sum = new WisdomDouble();
        Function<Event, Event> function;
        if (WisdomConfig.ASYNC_ENABLED) {
            // Thread safe operator
            function = event -> {
                synchronized (sum) {
                    if (event.isReset()) {
                        sum.set(0);
                    } else {
                        double sumValue = sum.addAndGet(event.getAsDouble(this.name));
                        event.set(newName, sumValue);
                    }
                }
                return event;
            };
        } else {
            function = event -> {
                if (event.isReset()) {
                    sum.set(0);
                } else {
                    double sumValue = sum.addAndGet(event.getAsDouble(this.name));
                    event.set(newName, sumValue);
                }
                return event;
            };
        }
        return function;

    }

    public Function<Event, Event> MIN_AS(String newName) {

        final WisdomReference<Comparable> min = new WisdomReference<>();
        final Comparator<Comparable> naturalOrder = Comparator.naturalOrder();
        final Comparator<Comparable> comparator = Comparator.nullsLast(naturalOrder);
        Function<Event, Event> function;
        if (WisdomConfig.ASYNC_ENABLED) {
            // Thread safe operator
            function = event -> {
                synchronized (min) {
                    if (event.isReset()) {
                        min.set(null);
                    } else {
                        min.setIfLess(comparator, event.get(this.name));
                        event.set(newName, min.get());
                    }
                }
                return event;
            };
        } else {
            function = event -> {
                if (event.isReset()) {
                    min.set(null);
                } else {
                    min.setIfLess(comparator, event.get(this.name));
                    event.set(newName, min.get());
                }
                return event;
            };
        }
        return function;
    }

    public Function<Event, Event> MAX_AS(String newName) {

        final WisdomReference<Comparable> max = new WisdomReference<>();
        final Comparator<Comparable> naturalOrder = Comparator.naturalOrder();
        final Comparator<Comparable> comparator = Comparator.nullsFirst(naturalOrder);
        Function<Event, Event> function;
        if (WisdomConfig.ASYNC_ENABLED) {
            // Thread safe operator
            function = event -> {
                synchronized (max) {
                    if (event.isReset()) {
                        max.set(null);
                    } else {
                        max.setIfGreater(comparator, event.get(this.name));
                        event.set(newName, max.get());
                    }
                }
                return event;
            };
        } else {
            function = event -> {
                if (event.isReset()) {
                    max.set(null);
                } else {
                    max.setIfGreater(comparator, event.get(this.name));
                    event.set(newName, max.get());
                }
                return event;
            };
        }
        return function;
    }

    public Function<Event, Event> AVG_AS(String newName) {

        final WisdomDouble sum = new WisdomDouble();
        final WisdomLong count = new WisdomLong();
        Function<Event, Event> function;
        if (WisdomConfig.ASYNC_ENABLED) {
            // Thread safe operator
            function = event -> {
                synchronized (sum) {
                    if (event.isReset()) {
                        sum.set(0);
                        count.set(0);
                    } else {
                        double total = sum.addAndGet(event.getAsDouble(this.name));
                        long noOfEvents = count.incrementAndGet();
                        double avg = total / noOfEvents;
                        if (avg != Double.NaN) {
                            avg = Math.round(avg * 10_000.0) / WisdomConfig.DOUBLE_PRECISION;
                        }
                        event.set(newName, avg);
                    }
                }
                return event;
            };
        } else {
            function = event -> {
                if (event.isReset()) {
                    sum.set(0);
                    count.set(0);
                } else {
                    double total = sum.addAndGet(event.getAsDouble(this.name));
                    long noOfEvents = count.incrementAndGet();
                    double avg = total / noOfEvents;
                    if (avg != Double.NaN) {
                        avg = Math.round(avg * 10_000.0) / WisdomConfig.DOUBLE_PRECISION;
                    }
                    event.set(newName, avg);
                }
                return event;
            };
        }
        return function;
    }

    public Predicate<Event> EQUALS(Object value) {

        return event -> Objects.equals(event.get(name), value);
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

    public Predicate<Event> STRING_MATCHES(String regex) {

        Pattern pattern = Pattern.compile(regex);

        return event -> {
            String data = (String) event.get(name);
            if (data == null) {
                return false;
            } else {
                Matcher matcher = pattern.matcher(data);
                return matcher.find();
            }
        };
    }
}
