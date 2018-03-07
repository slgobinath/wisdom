package com.javahelps.wisdom.core.operator;

import com.javahelps.wisdom.core.event.Event;
import com.javahelps.wisdom.core.partition.Partitionable;
import com.javahelps.wisdom.core.util.WisdomConfig;

public class AvgOperator extends AggregateOperator {

    private double sum;
    private long count;

    public AvgOperator(String attribute, String as) {
        super(attribute, as);
    }

    @Override
    public Comparable apply(Event event) {
        double value;
        synchronized (this) {
            if (event.isReset()) {
                sum = 0.0;
                count = 0L;
                value = 0.0;
            } else {
                this.sum += event.getAsDouble(attribute);
                this.count++;
                value = this.sum / this.count;
                if (value != Double.NaN) {
                    value = Math.round(value * WisdomConfig.DOUBLE_PRECISION) / WisdomConfig.DOUBLE_PRECISION;
                }
            }
        }
        return value;
    }

    @Override
    public void clear() {
        synchronized (this) {
            this.sum = 0.0;
            this.count = 0L;
        }
    }

    @Override
    public Partitionable copy() {
        return new AvgOperator(this.attribute, this.newName);
    }
}
