package com.javahelps.wisdom.core.operator;

import com.javahelps.wisdom.core.event.Event;
import com.javahelps.wisdom.core.partition.Partitionable;

import java.util.Comparator;

public class MinOperator extends AggregateOperator {

    private Comparable min;
    private final Comparator<Comparable> naturalOrder = Comparator.naturalOrder();
    private final Comparator<Comparable> comparator = Comparator.nullsLast(naturalOrder);

    public MinOperator(String attribute, String as) {
        super(attribute, as);
    }

    @Override
    public Comparable apply(Event event) {
        Comparable value;
        synchronized (this) {
            if (event.isReset()) {
                value = null;
            } else {
                Comparable newReference = event.get(attribute);
                if (comparator.compare(newReference, this.min) < 0) {
                    this.min = newReference;
                }
                value = this.min;
            }
        }
        return value;
    }

    @Override
    public void clear() {
        synchronized (this) {
            this.min = null;
        }
    }

    @Override
    public Partitionable copy() {
        return new MinOperator(this.attribute, this.newName);
    }
}
