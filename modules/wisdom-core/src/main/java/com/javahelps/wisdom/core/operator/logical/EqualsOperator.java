package com.javahelps.wisdom.core.operator.logical;

import com.javahelps.wisdom.core.event.Event;
import com.javahelps.wisdom.core.exception.WisdomAppValidationException;
import com.javahelps.wisdom.core.extension.WisdomExtension;
import com.javahelps.wisdom.core.util.Commons;

import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;

@WisdomExtension("==")
public class EqualsOperator extends LogicalOperator {

    private Predicate<Event> predicate;

    public EqualsOperator(Map<String, ?> properties) {
        super(properties);
        Object left = Commons.getProperty(properties, "left", 0);
        Object right = Commons.getProperty(properties, "right", 1);

        if (left instanceof Comparable) {
            // constant == right
            if (right instanceof Comparable) {
                // constant == constant
                boolean result = test(left, right);
                predicate = event -> result;
            } else if (right instanceof Function) {
                // constant == function(attribute)
                predicate = event -> test(left, ((Function) right).apply(event));
            } else if (right instanceof Supplier) {
                // constant == supplier(variable)
                predicate = event -> test(left, ((Supplier) right).get());
            } else if (right == null) {
                // constant == null
                boolean result = left == null;
                predicate = event -> result;
            } else {
                throw new WisdomAppValidationException("java.lang.Comparable == %s is not supported", right.getClass().getCanonicalName());
            }
        } else if (left instanceof Function) {
            // function(attribute) == right
            if (right instanceof Comparable) {
                // function(attribute) == constant
                predicate = event -> test(((Function) left).apply(event), right);
            } else if (right instanceof Function) {
                // function(attribute) == function(attribute)
                predicate = event -> test(((Function) left).apply(event), ((Function) right).apply(event));
            } else if (right instanceof Supplier) {
                // function(attribute) == supplier(variable)
                predicate = event -> test(((Function) left).apply(event), ((Supplier) right).get());
            } else if (right == null) {
                // function(attribute) == null
                predicate = event -> ((Function) left).apply(event) == null;
            } else {
                throw new WisdomAppValidationException("java.util.function.Function == %s is not supported", right.getClass().getCanonicalName());
            }
        } else if (left instanceof Supplier) {
            // supplier(variable) == right
            if (right instanceof Comparable) {
                // supplier(variable) == constant
                predicate = event -> test(((Supplier) left).get(), right);
            } else if (right instanceof Function) {
                // supplier(variable) == function(attribute)
                predicate = event -> test(((Supplier) left).get(), ((Function) right).apply(event));
            } else if (right instanceof Supplier) {
                // supplier(variable) == supplier(variable)
                predicate = event -> test(((Supplier) left).get(), ((Supplier) right).get());
            } else if (right == null) {
                // supplier(variable) == null
                predicate = event -> ((Supplier) left).get() == null;
            } else {
                throw new WisdomAppValidationException("java.util.function.Supplier == %s is not supported", right.getClass().getCanonicalName());
            }
        } else {
            throw new WisdomAppValidationException("%s == is not supported", right.getClass().getCanonicalName());
        }
    }

    private static boolean test(Object left, Object right) {
        if (left instanceof Number && right instanceof Number) {
            return ((Number) left).doubleValue() == ((Number) right).doubleValue();
        } else {
            return Objects.equals(left, right);
        }
    }

    @Override
    public boolean test(Event event) {
        return this.predicate.test(event);
    }
}
