package com.javahelps.wisdom.core.operator.logical;

import com.javahelps.wisdom.core.event.Event;
import com.javahelps.wisdom.core.exception.WisdomAppRuntimeException;
import com.javahelps.wisdom.core.exception.WisdomAppValidationException;
import com.javahelps.wisdom.core.extension.WisdomExtension;
import com.javahelps.wisdom.core.operand.WisdomArray;
import com.javahelps.wisdom.core.util.Commons;

import java.util.Map;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;

@WisdomExtension("in")
public class InOperator extends LogicalOperator {

    private Predicate<Event> predicate;

    public InOperator(Map<String, ?> properties) {
        super(properties);
        Object left = Commons.getProperty(properties, "left", 0);
        Object right = Commons.getProperty(properties, "right", 1);

        if (right instanceof WisdomArray) {
            // left in array
            if (left instanceof Comparable) {
                // constant in array
                final boolean result = ((WisdomArray) right).contains((Comparable) left);
                predicate = event -> result;
            } else if (left instanceof Function) {
                // function(attribute) in array
                predicate = event -> ((WisdomArray) right).contains((Comparable) ((Function) left).apply(event));
            } else if (left instanceof Supplier) {
                // supplier(variable) in array
                predicate = event -> ((WisdomArray) right).contains((Comparable) ((Supplier) left).get());
            } else {
                throw new WisdomAppValidationException("%s IN WisdomArray is not supported", left.getClass().getCanonicalName());
            }
        } else if (right instanceof String) {
            // left in String
            if (left instanceof String) {
                // String in String
                final boolean result = ((String) right).contains((CharSequence) left);
                predicate = event -> result;
            } else if (left instanceof Function) {
                // function(attribute) in String
                predicate = event -> {
                    Object value = ((Function) left).apply(event);
                    if (value instanceof String) {
                        return ((String) right).contains((String) value);
                    } else {
                        return false;
                    }
                };
            } else if (left instanceof Supplier) {
                // supplier(variable) in String
                predicate = event -> {
                    Object value = ((Supplier) left).get();
                    if (value instanceof String) {
                        return ((String) right).contains((String) value);
                    } else {
                        return false;
                    }
                };
            } else {
                throw new WisdomAppValidationException("%s IN java.lang.String is not supported", left.getClass().getCanonicalName());
            }
        } else if (right instanceof Function) {
            // left in function(attribute)
            if (left instanceof Comparable) {
                // constant in function(attribute)
                predicate = event -> {
                    Comparable container = (Comparable) ((Function) right).apply(event);
                    return test((Comparable) left, container);
                };
            } else if (left instanceof Function) {
                // function(attribute) in function(attribute)
                predicate = event -> {
                    Comparable value = (Comparable) ((Function) left).apply(event);
                    Comparable container = (Comparable) ((Function) right).apply(event);
                    return test(value, container);
                };
            } else if (left instanceof Supplier) {
                // supplier(variable) in function(attribute)
                predicate = event -> {
                    Comparable value = (Comparable) ((Supplier) left).get();
                    Comparable container = (Comparable) ((Function) right).apply(event);
                    return test(value, container);
                };
            } else {
                throw new WisdomAppValidationException("%s IN java.util.function.Function is not supported", left.getClass().getCanonicalName());
            }
        } else if (right instanceof Supplier) {
            // left in supplier(variable)
            if (left instanceof Comparable) {
                // constant in supplier(variable)
                predicate = event -> {
                    Comparable container = (Comparable) ((Supplier) right).get();
                    return test((Comparable) left, container);
                };
            } else if (left instanceof Function) {
                // function(attribute) in supplier(variable)
                predicate = event -> {
                    Comparable value = (Comparable) ((Function) left).apply(event);
                    Comparable container = (Comparable) ((Supplier) right).get();
                    return test(value, container);
                };
            } else if (left instanceof Supplier) {
                // supplier(variable) in supplier(variable)
                predicate = event -> {
                    Comparable value = (Comparable) ((Supplier) left).get();
                    Comparable container = (Comparable) ((Supplier) right).get();
                    return test(value, container);
                };
            } else {
                throw new WisdomAppValidationException("%s IN java.util.function.Supplier is not supported", left.getClass().getCanonicalName());
            }
        } else {
            throw new WisdomAppValidationException("IN %s is not supported", right.getClass().getCanonicalName());
        }
    }

    @Override
    public boolean test(Event event) {
        return this.predicate.test(event);
    }

    private boolean test(Comparable value, Comparable container) {
        if (container == null) {
            return false;
        }
        if (container instanceof WisdomArray) {
            return ((WisdomArray) container).contains(value);
        } else if (container instanceof String) {
            if (value instanceof String) {
                return ((String) container).contains((CharSequence) value);
            } else {
                return false;
            }
        } else {
            throw new WisdomAppRuntimeException("%s IN %s is not supported", value, container);
        }
    }
}
