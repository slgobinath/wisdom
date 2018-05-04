package com.javahelps.wisdom.core.operator;

import com.javahelps.wisdom.core.event.Event;
import com.javahelps.wisdom.core.partition.Partitionable;

import java.util.Comparator;

public class MaxOperator extends AggregateOperator {

    private final Comparator<Comparable> naturalOrder = Comparator.naturalOrder();
    private final Comparator<Comparable> comparator = Comparator.nullsFirst(naturalOrder);
    private Comparable max;

    public MaxOperator(String attribute, String as) {
        super(attribute, as);
    }

    @Override
    public Comparable apply(Event event) {
        Comparable value;
        synchronized (this) {
            if (event.isReset()) {
                value = null;
            } else {
                Comparable newReference = (Comparable) event.get(attribute);
                if (comparator.compare(newReference, this.max) > 0) {
                    this.max = newReference;
                }
                value = this.max;
            }
        }
        return value;
    }

    @Override
    public void clear() {
        synchronized (this) {
            this.max = null;
        }
    }

    @Override
    public Partitionable copy() {
        return new MaxOperator(this.attribute, this.newName);
    }

    @Override
    public void destroy() {
        this.max = null;
    }
}
