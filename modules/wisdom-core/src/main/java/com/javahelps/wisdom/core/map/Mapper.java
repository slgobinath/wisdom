package com.javahelps.wisdom.core.map;

import com.javahelps.wisdom.core.event.Event;
import com.javahelps.wisdom.core.extension.ImportsManager;

import java.util.Collections;
import java.util.Map;
import java.util.function.Function;

public abstract class Mapper implements Function<Event, Event> {

    protected final String currentName;
    protected final String newName;

    static {
        ImportsManager.INSTANCE.use(Mapper.class.getPackageName());
    }

    public Mapper(String currentName, String newName, Map<String, ?> properties) {
        this.currentName = currentName;
        this.newName = newName;
    }

    public static Mapper create(String namespace, String currentName, String newName, Map<String, ?> properties) {
        return ImportsManager.INSTANCE.createMapper(namespace, currentName, newName, properties);
    }

    public static Mapper formatTime(String currentName, String newName) {
        return new FormatTimeMapper(currentName, newName, Collections.emptyMap());
    }

    public static Mapper rename(String currentName, String newName) {
        return new RenameMapper(currentName, newName, Collections.emptyMap());
    }
}
