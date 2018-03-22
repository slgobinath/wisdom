package com.javahelps.wisdom.core.operator;

import com.javahelps.wisdom.core.event.Event;
import com.javahelps.wisdom.core.operand.WisdomLong;
import com.javahelps.wisdom.core.processor.AttributeSelectProcessor;

import java.util.Objects;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * {@link Operator} provides some built-in operations on the given attribute of an {@link Event} at the
 * runtime.
 * {@link Operator} modifies the attributes of the {@link Event} which is passed newName the parameter of the
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


    public static AggregateOperator SUM(final String attribute, final String as) {

        return new SumOperator(attribute, as);
    }

    public static AggregateOperator AVG(final String attribute, final String as) {

        return new AvgOperator(attribute, as);
    }

    public static AggregateOperator MIN(final String attribute, final String as) {

        return new MinOperator(attribute, as);
    }

    public static AggregateOperator MAX(final String attribute, final String as) {

        return new MaxOperator(attribute, as);
    }

    public static AggregateOperator COUNT(String as) {

        return new CountOperator(as);
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
}