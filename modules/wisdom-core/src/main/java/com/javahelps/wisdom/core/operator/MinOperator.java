package com.javahelps.wisdom.core.operator;

import com.javahelps.wisdom.core.event.Event;
import com.javahelps.wisdom.core.exception.WisdomAppValidationException;
import com.javahelps.wisdom.core.extension.WisdomExtension;
import com.javahelps.wisdom.core.partition.Partitionable;
import com.javahelps.wisdom.core.util.Commons;

import java.util.Comparator;
import java.util.Map;

import static com.javahelps.wisdom.core.util.WisdomConstants.ATTR;

@WisdomExtension("min")
public class MinOperator extends AggregateOperator {

    private final Comparator<Comparable> naturalOrder = Comparator.naturalOrder();
    private final Comparator<Comparable> comparator = Comparator.nullsLast(naturalOrder);
    private String attribute;
    private Comparable min;

    public MinOperator(String as, Map<String, ?> properties) {
        super(as, properties);
        this.attribute = Commons.getProperty(properties, ATTR, 0);
        if (this.attribute == null) {
            throw new WisdomAppValidationException("Required property %s of Min operator not found", ATTR);
        }
    }

    @Override
    public Object apply(Event event) {
        Comparable value;
        synchronized (this) {
            if (event.isReset()) {
                value = null;
            } else {
                Comparable newReference = (Comparable) event.get(attribute);
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
        return new MinOperator(this.newName, Map.of(ATTR, this.attribute));
    }

    @Override
    public void destroy() {
        this.min = null;
    }
}
