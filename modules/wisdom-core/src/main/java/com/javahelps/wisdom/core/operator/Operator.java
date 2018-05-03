package com.javahelps.wisdom.core.operator;

import com.javahelps.wisdom.core.event.Event;
import com.javahelps.wisdom.core.operator.logical.*;
import com.javahelps.wisdom.core.processor.AttributeSelectProcessor;

import java.util.Map;
import java.util.function.Function;

/**
 * {@link Operator} provides some built-in operations on the given attribute of an {@link Event} at the
 * runtime.
 * {@link Operator} modifies the attributes of the {@link Event} which is passed attrName the parameter of the
 * {@link Function}.
 *
 * @see AttributeSelectProcessor
 */
public class Operator {

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

    public static LogicalOperator IN(Object left, Object right) {
        return new InOperator(Map.of("left", left, "right", right));
    }

    public static LogicalOperator MATCHES(Object regex, Object right) {
        return new RegexInOperator(Map.of("left", regex, "right", right));
    }

    public static LogicalOperator EQUALS(Object left, Object right) {
        return new EqualsOperator(Map.of("left", left, "right", right));
    }

    public static LogicalOperator GREATER_THAN(Object left, Object right) {
        return new GreaterThanOperator(Map.of("left", left, "right", right));
    }

    public static LogicalOperator GREATER_THAN_OR_EQUAL(Object left, Object right) {
        return new GreaterThanOrEqualOperator(Map.of("left", left, "right", right));
    }

    public static LogicalOperator LESS_THAN(Object left, Object right) {
        return new LessThanOperator(Map.of("left", left, "right", right));
    }

    public static LogicalOperator LESS_THAN_OR_EQUAL(Object left, Object right) {
        return new LessThanOrEqualOperator(Map.of("left", left, "right", right));
    }
}
