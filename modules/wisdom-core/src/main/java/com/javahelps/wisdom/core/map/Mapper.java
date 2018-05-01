package com.javahelps.wisdom.core.map;

import com.javahelps.wisdom.core.WisdomApp;
import com.javahelps.wisdom.core.event.Event;
import com.javahelps.wisdom.core.extension.ImportsManager;

import java.util.Collections;
import java.util.Map;
import java.util.function.Function;

import static com.javahelps.wisdom.core.util.WisdomConstants.ATTR;

public abstract class Mapper implements Function<Event, Event> {

    protected final String attrName;

    static {
        ImportsManager.INSTANCE.use(Mapper.class.getPackageName());
    }

    public Mapper(String attrName, Map<String, ?> properties) {
        this.attrName = attrName;
    }

    public abstract void start();

    public abstract void init(WisdomApp wisdomApp);

    public abstract void stop();

    public static Mapper create(String namespace, String newName, Map<String, ?> properties) {
        return ImportsManager.INSTANCE.createMapper(namespace, newName, properties);
    }

    public static Mapper formatTime(String currentName, String newName) {
        return new FormatTimeMapper(newName, Map.of(ATTR, currentName));
    }

    public static Mapper rename(String currentName, String newName) {
        return new RenameMapper(newName, Map.of(ATTR, currentName));
    }
}
