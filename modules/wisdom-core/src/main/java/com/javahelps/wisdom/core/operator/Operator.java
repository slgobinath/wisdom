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
 * {@link Operator} provides some built-in operations on the given attribute of an {@link Event} at the
 * runtime.
 * {@link Operator} modifies the attributes of the {@link Event} which is passed as the parameter of the
 * {@link Function}.
 *
 * @see AttributeSelectProcessor
 */
public class Operator {

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

    public static Function<Event, Comparable> SUM(final String attribute) {

        final WisdomDouble sum = new WisdomDouble();
        Function<Event, Comparable> function = event -> {
            Double value;
            synchronized (sum) {
                if (event.isReset()) {
                    value = sum.set(0);
                } else {
                    value = sum.addAndGet(event.getAsDouble(attribute));
                }
            }
            return value;
        };
        return function;

    }

    public static Function<Event, Comparable> MIN(final String attribute) {

        final WisdomReference<Comparable> min = new WisdomReference<>();
        final Comparator<Comparable> naturalOrder = Comparator.naturalOrder();
        final Comparator<Comparable> comparator = Comparator.nullsLast(naturalOrder);
        Function<Event, Comparable> function = event -> {
            Comparable value;
            synchronized (min) {
                if (event.isReset()) {
                    value = min.set(null);
                } else {
                    value = min.setIfLess(comparator, event.get(attribute));
                }
            }
            return value;
        };
        return function;
    }

    public static Function<Event, Comparable> MAX(final String attribute) {

        final WisdomReference<Comparable> max = new WisdomReference<>();
        final Comparator<Comparable> naturalOrder = Comparator.naturalOrder();
        final Comparator<Comparable> comparator = Comparator.nullsFirst(naturalOrder);
        Function<Event, Comparable> function = event -> {
            Comparable value;
            synchronized (max) {
                if (event.isReset()) {
                    value = max.set(null);
                } else {
                    value = max.setIfGreater(comparator, event.get(attribute));
                }
            }
            return value;
        };

        return function;
    }

    public static Function<Event, Comparable> AVG(final String attribute) {

        final WisdomDouble sum = new WisdomDouble();
        final WisdomLong count = new WisdomLong();
        Function<Event, Comparable> function = event -> {
            Double value;
            synchronized (sum) {
                if (event.isReset()) {
                    sum.set(0);
                    count.set(0);
                    value = 0.0;
                } else {
                    double total = sum.addAndGet(event.getAsDouble(attribute));
                    long noOfEvents = count.incrementAndGet();
                    double avg = total / noOfEvents;
                    if (avg != Double.NaN) {
                        avg = Math.round(avg * 10_000.0) / WisdomConfig.DOUBLE_PRECISION;
                    }
                    value = avg;
                }
            }
            return value;
        };
        return function;
    }

    public static Predicate<Event> EQUALS(final String attribute, final Comparable value) {

        if (value instanceof Long || value instanceof Integer) {
            final long longValue = ((Number) value).longValue();
            return event -> event.getAsDouble(attribute) == longValue;
        } else if (value instanceof Double || value instanceof Float) {
            final double doubleValue = ((Number) value).doubleValue();
            return event -> event.getAsDouble(attribute) == doubleValue;
        } else {
            return event -> Objects.equals(event.get(attribute), value);
        }
    }

    public static Predicate<Event> EQUALS(final Supplier<Comparable> supplier, final Comparable value) {

        if (value instanceof Long || value instanceof Integer) {
            final long longValue = ((Number) value).longValue();
            return event -> ((Number) supplier.get()).longValue() == longValue;
        } else if (value instanceof Double || value instanceof Float) {
            final double doubleValue = ((Number) value).doubleValue();
            return event -> ((Number) supplier.get()).doubleValue() == doubleValue;
        } else {
            return event -> Objects.equals(supplier.get(), value);
        }
    }

    public static Predicate<Event> EQUALS(final String attribute, final Supplier<Comparable> right) {

        return event -> Objects.equals(event.get(attribute), right.get());
    }


    public static Predicate<Event> EQUALS(final Supplier<Comparable> left, final Supplier<Comparable> right) {

        return event -> Objects.equals(left.get(), right.get());
    }

    public static Predicate<Event> EQUAL_ATTRIBUTES(final String attrOne, final String attrTwo) {

        return event -> Objects.equals(event.get(attrOne), event.get(attrTwo));
    }

    public static Predicate<Event> GREATER_THAN(final String leftAttr, final String rightAttr) {

        return event -> event.getAsDouble(leftAttr) > event.getAsDouble(rightAttr);
    }

    public static Predicate<Event> GREATER_THAN(final String attribute, final Supplier<Comparable> right) {

        return event -> event.getAsDouble(attribute) > ((Number) right.get()).doubleValue();
    }

    public static Predicate<Event> GREATER_THAN(final Supplier<Comparable> left, final Supplier<Comparable> right) {

        return event -> ((Number) left.get()).doubleValue() > ((Number) right.get()).doubleValue();
    }

    public static Predicate<Event> GREATER_THAN(final Supplier<Comparable> left, final double value) {

        return event -> ((Number) left.get()).doubleValue() > value;
    }

    public static Predicate<Event> GREATER_THAN(final String attribute, final double value) {

        return event -> event.getAsDouble(attribute) > value;
    }


    public static Predicate<Event> GREATER_THAN_OR_EQUAL(final String leftAttr, final String rightAttr) {

        return event -> event.getAsDouble(leftAttr) >= event.getAsDouble(rightAttr);
    }

    public static Predicate<Event> GREATER_THAN_OR_EQUAL(final String attribute, final Supplier<Comparable> right) {

        return event -> event.getAsDouble(attribute) >= ((Number) right.get()).doubleValue();
    }

    public static Predicate<Event> GREATER_THAN_OR_EQUAL(final Supplier<Comparable> left, final Supplier<Comparable>
            right) {

        return event -> ((Number) left.get()).doubleValue() >= ((Number) right.get()).doubleValue();
    }

    public static Predicate<Event> GREATER_THAN_OR_EQUAL(final Supplier<Comparable> left, final double value) {

        return event -> ((Number) left.get()).doubleValue() >= value;
    }

    public static Predicate<Event> GREATER_THAN_OR_EQUAL(final String attribute, final double value) {

        return event -> event.getAsDouble(attribute) >= value;
    }


    public static Predicate<Event> LESS_THAN(final String leftAttr, final String rightAttr) {

        return event -> event.getAsDouble(leftAttr) < event.getAsDouble(rightAttr);
    }

    public static Predicate<Event> LESS_THAN(final String attribute, final Supplier<Comparable> right) {

        return event -> event.getAsDouble(attribute) < ((Number) right.get()).doubleValue();
    }

    public static Predicate<Event> LESS_THAN(final Supplier<Comparable> left, final Supplier<Comparable> right) {

        return event -> ((Number) left.get()).doubleValue() < ((Number) right.get()).doubleValue();
    }

    public static Predicate<Event> LESS_THAN(final String attribute, final double value) {

        return event -> event.getAsDouble(attribute) < value;
    }

    public static Predicate<Event> LESS_THAN(final Supplier<Comparable> left, final double value) {

        return event -> ((Number) left.get()).doubleValue() < value;
    }


    public static Predicate<Event> LESS_THAN_OR_EQUAL(final String leftAttr, final String rightAttr) {

        return event -> event.getAsDouble(leftAttr) <= event.getAsDouble(rightAttr);
    }

    public static Predicate<Event> LESS_THAN_OR_EQUAL(final String attribute, final Supplier<Comparable> right) {

        return event -> event.getAsDouble(attribute) <= ((Number) right.get()).doubleValue();
    }

    public static Predicate<Event> LESS_THAN_OR_EQUAL(final Supplier<Comparable> left, final Supplier<Comparable>
            right) {

        return event -> ((Number) left.get()).doubleValue() <= ((Number) right.get()).doubleValue();
    }

    public static Predicate<Event> LESS_THAN_OR_EQUAL(final String attribute, final double value) {

        return event -> event.getAsDouble(attribute) <= value;
    }

    public static Predicate<Event> LESS_THAN_OR_EQUAL(final Supplier<Comparable> left, final double value) {

        return event -> ((Number) left.get()).doubleValue() <= value;
    }

    public static Predicate<Event> STRING_MATCHES(final String attribute, final String regex) {

        Pattern pattern = Pattern.compile(regex);

        return event -> {
            String data = (String) event.get(attribute);
            if (data == null) {
                return false;
            } else {
                Matcher matcher = pattern.matcher(data);
                return matcher.find();
            }
        };
    }

    public static final Function<Event, Comparable> COUNT() {

        final WisdomLong count = new WisdomLong();
        Function<Event, Comparable> function = event -> {
            long value;
            synchronized (count) {
                if (event.isReset()) {
                    value = count.set(0);
                } else {
                    value = count.incrementAndGet();
                }
            }
            return value;
        };
        return function;
    }
}
