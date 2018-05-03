package com.javahelps.wisdom.core.operator.logical;

import com.javahelps.wisdom.core.event.Event;
import com.javahelps.wisdom.core.extension.ImportsManager;

import java.util.Map;
import java.util.function.Predicate;

public abstract class LogicalOperator implements Predicate<Event> {

    static {
        ImportsManager.INSTANCE.use(LogicalOperator.class.getPackageName());
    }

    public LogicalOperator(Map<String, ?> properties) {

    }

    public static LogicalOperator create(String namespace, Map<String, ?> properties) {
        return ImportsManager.INSTANCE.createLogicalOperator(namespace, properties);
    }
}
