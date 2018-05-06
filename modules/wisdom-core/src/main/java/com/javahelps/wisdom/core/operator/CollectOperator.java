package com.javahelps.wisdom.core.operator;

import com.javahelps.wisdom.core.event.Event;
import com.javahelps.wisdom.core.exception.WisdomAppValidationException;
import com.javahelps.wisdom.core.extension.WisdomExtension;
import com.javahelps.wisdom.core.operand.WisdomArray;
import com.javahelps.wisdom.core.partition.Partitionable;
import com.javahelps.wisdom.core.util.Commons;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static com.javahelps.wisdom.core.util.WisdomConstants.ATTR;

@WisdomExtension("collect")
public class CollectOperator extends AggregateOperator {

    private String attribute;
    private List<Object> values = new ArrayList<>();

    public CollectOperator(String as, Map<String, ?> properties) {
        super(as, properties);
        this.attribute = Commons.getProperty(properties, ATTR, 0);
        if (this.attribute == null) {
            throw new WisdomAppValidationException("Required property %s of collect operator not found", ATTR);
        }
    }

    @Override
    public Object apply(Event event) {
        WisdomArray value;
        synchronized (this) {
            if (event.isReset()) {
                this.values.clear();
            } else {
                this.values.add(event.get(this.attribute));
            }
            value = WisdomArray.of(this.values);
        }
        return value;
    }

    @Override
    public void clear() {
        synchronized (this) {
            this.values.clear();
        }
    }

    @Override
    public void destroy() {
        this.values = null;
    }

    @Override
    public Partitionable copy() {
        return new CollectOperator(this.newName, Map.of(ATTR, this.attribute));
    }
}
