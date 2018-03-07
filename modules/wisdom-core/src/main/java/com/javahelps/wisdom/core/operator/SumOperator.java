package com.javahelps.wisdom.core.operator;

import com.javahelps.wisdom.core.event.Event;
import com.javahelps.wisdom.core.partition.Partitionable;

public class SumOperator extends AggregateOperator {

    private double sum;

    public SumOperator(String attribute, String as) {
        super(attribute, as);
    }

    @Override
    public Comparable apply(Event event) {
        double value;
        synchronized (this) {
            if (event.isReset()) {
                this.sum = 0.0;
            } else {
                this.sum += event.getAsDouble(this.attribute);
            }
            value = this.sum;
        }
        return value;
    }

    @Override
    public void clear() {
        synchronized (this) {
            this.sum = 0.0;
        }
    }

    @Override
    public Partitionable copy() {
        return new SumOperator(this.attribute, this.newName);
    }
}
