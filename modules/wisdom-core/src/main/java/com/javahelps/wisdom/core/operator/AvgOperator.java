package com.javahelps.wisdom.core.operator;

import com.javahelps.wisdom.core.event.Event;
import com.javahelps.wisdom.core.exception.WisdomAppValidationException;
import com.javahelps.wisdom.core.extension.WisdomExtension;
import com.javahelps.wisdom.core.partition.Partitionable;
import com.javahelps.wisdom.core.util.Commons;
import com.javahelps.wisdom.core.util.WisdomConfig;

import java.util.Map;

import static com.javahelps.wisdom.core.util.WisdomConstants.ATTR;

@WisdomExtension("avg")
public class AvgOperator extends AggregateOperator {

    private String attribute;
    private double sum;
    private long count;


    public AvgOperator(String as, Map<String, ?> properties) {
        super(as, properties);
        this.attribute = Commons.getProperty(properties, ATTR, 0);
        if (this.attribute == null) {
            throw new WisdomAppValidationException("Required property %s of Avg operator not found", ATTR);
        }
    }

    @Override
    public Object apply(Event event) {
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
        return new AvgOperator(this.newName, Map.of(ATTR, this.attribute));
    }

    @Override
    public void destroy() {

    }
}
