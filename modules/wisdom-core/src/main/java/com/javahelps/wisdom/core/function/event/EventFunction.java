package com.javahelps.wisdom.core.function.event;

import com.javahelps.wisdom.core.event.Event;
import com.javahelps.wisdom.core.extension.ImportsManager;

import java.util.Map;
import java.util.function.Function;

public abstract class EventFunction implements Function<Event, Object> {

    static {
        ImportsManager.INSTANCE.use(EventFunction.class.getPackageName());
    }

    public EventFunction(Map<String, ?> properties) {

    }

    public static EventFunction create(String namespace, Map<String, ?> properties) {
        return ImportsManager.INSTANCE.createEventFunction(namespace, properties);
    }
}
