package com.javahelps.wisdom.core.map;

import com.javahelps.wisdom.core.WisdomApp;
import com.javahelps.wisdom.core.event.Event;
import com.javahelps.wisdom.core.extension.ImportsManager;

import java.util.Map;
import java.util.function.Function;
import java.util.function.Predicate;

import static com.javahelps.wisdom.core.util.WisdomConstants.ATTR;
import static com.javahelps.wisdom.core.util.WisdomConstants.VALUE;

public abstract class Mapper implements Function<Event, Event> {

    static {
        ImportsManager.INSTANCE.use(Mapper.class.getPackageName());
    }

    protected final String attrName;
    private Predicate<Event> predicate = event -> true;

    public Mapper(String attrName, Map<String, ?> properties) {
        this.attrName = attrName;
    }

    public static Mapper create(String namespace, String newName, Map<String, ?> properties) {
        return ImportsManager.INSTANCE.createMapper(namespace, newName, properties);
    }

    public static Mapper FORMAT_TIME(String currentName, String newName) {
        return new FormatTimeMapper(newName, Map.of(ATTR, currentName));
    }

    public static Mapper LENGTH(String currentName, String newName) {
        return new LengthMapper(newName, Map.of(ATTR, currentName));
    }

    public static Mapper RENAME(String currentName, String newName) {
        return new RenameMapper(newName, Map.of(ATTR, currentName));
    }

    public static Mapper TO_INT(String currentName, String newName) {
        return new IntMapper(newName, Map.of(ATTR, currentName));
    }

    public static Mapper TO_DOUBLE(String currentName, String newName) {
        return new DoubleMapper(newName, Map.of(ATTR, currentName));
    }

    public static Mapper CONSTANT(Object value, String newName) {
        return new ConstantMapper(newName, Map.of(VALUE, value));
    }

    public abstract void start();

    public abstract void init(WisdomApp wisdomApp);

    public abstract void stop();

    public abstract Event map(Event event);

    @Override
    public Event apply(Event event) {
        if (this.predicate.test(event)) {
            return this.map(event);
        } else {
            return event;
        }
    }

    public Mapper onlyIf(Predicate<Event> predicate) {
        this.predicate = predicate;
        return this;
    }
}
