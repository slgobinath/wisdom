package com.javahelps.wisdom.core.operator;

import com.javahelps.wisdom.core.event.Event;
import com.javahelps.wisdom.core.partition.Partitionable;

public class CountOperator extends AggregateOperator {

    private long count;

    public CountOperator(String as) {
        super("", as);
    }

    @Override
    public Comparable apply(Event event) {
        long value;
        synchronized (this) {
            if (event.isReset()) {
                this.count = 0;
            } else {
                this.count++;
            }
            value = this.count;
        }
        return value;
    }

    @Override
    public void clear() {
        synchronized (this) {
            this.count = 0L;
        }
    }

    @Override
    public Partitionable copy() {
        return new CountOperator(this.newName);
    }

    @Override
    public void destroy() {

    }
}
