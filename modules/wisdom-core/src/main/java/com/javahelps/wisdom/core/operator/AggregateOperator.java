package com.javahelps.wisdom.core.operator;

import com.javahelps.wisdom.core.event.Event;
import com.javahelps.wisdom.core.partition.Partitionable;
import com.javahelps.wisdom.core.processor.Stateful;
import com.javahelps.wisdom.core.variable.Variable;

import java.util.function.Function;

public abstract class AggregateOperator implements Function<Event, Comparable>, Stateful, Partitionable {

    protected String attribute;
    protected Variable variable;
    protected final String newName;

    protected AggregateOperator(String attribute, String as) {
        this.attribute = attribute;
        this.newName = as;
    }

    public String getNewName() {
        return newName;
    }
}
