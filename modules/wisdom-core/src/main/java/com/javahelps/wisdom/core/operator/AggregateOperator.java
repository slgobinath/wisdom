package com.javahelps.wisdom.core.operator;

import com.javahelps.wisdom.core.event.Event;
import com.javahelps.wisdom.core.extension.ImportsManager;
import com.javahelps.wisdom.core.partition.Partitionable;
import com.javahelps.wisdom.core.processor.Stateful;

import java.util.Map;
import java.util.function.Function;

public abstract class AggregateOperator implements Function<Event, Object>, Stateful, Partitionable {

    protected final String newName;

    static {
        ImportsManager.INSTANCE.use(AggregateOperator.class.getPackageName());
    }

    protected AggregateOperator(String as, Map<String, ?> properties) {
        this.newName = as;
    }

    public String getNewName() {
        return newName;
    }

    public static AggregateOperator create(String namespace, String as, Map<String, ?> properties) {
        return ImportsManager.INSTANCE.createAggregateOperator(namespace, as, properties);
    }
}
