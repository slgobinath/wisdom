package com.javahelps.wisdom.core.operator;

import com.javahelps.wisdom.core.event.Event;
import com.javahelps.wisdom.core.extension.WisdomExtension;
import com.javahelps.wisdom.core.partition.Partitionable;

import java.util.Collections;
import java.util.Map;

@WisdomExtension("count")
public class CountOperator extends AggregateOperator {

    private long count;

    public CountOperator(String as, Map<String, ?> properties) {
        super(as, properties);
    }

    @Override
    public Object apply(Event event) {
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
        return new CountOperator(this.newName, Collections.EMPTY_MAP);
    }

    @Override
    public void destroy() {

    }
}
