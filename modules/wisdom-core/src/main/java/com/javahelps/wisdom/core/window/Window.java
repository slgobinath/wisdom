package com.javahelps.wisdom.core.window;

import com.javahelps.wisdom.core.event.Event;
import com.javahelps.wisdom.core.exception.WisdomAppValidationException;
import com.javahelps.wisdom.core.extension.ImportsManager;
import com.javahelps.wisdom.core.partition.Partitionable;
import com.javahelps.wisdom.core.processor.Processor;
import com.javahelps.wisdom.core.processor.Stateful;
import com.javahelps.wisdom.core.util.Commons;
import com.javahelps.wisdom.core.variable.Variable;

import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * A utility to construct Windows.
 */
public abstract class Window implements Partitionable, Stateful {

    protected final Lock lock = new ReentrantLock();
    protected final Map<String, ?> properties;

    public Window(Map<String, ?> properties) {
        this.properties = properties;
    }

    static {
        ImportsManager.INSTANCE.use(Window.class.getPackageName());
    }

    public static Window create(String namespace, Map<String, ?> properties) {
        return ImportsManager.INSTANCE.createWindow(namespace, properties);
    }

    public static Window length(int length) {
        return new LengthWindow(Collections.singletonMap("length", length));
    }

    public static Window length(Variable<Integer> length) {
        return new LengthWindow(Collections.singletonMap("length", length));
    }

    public static Window lengthBatch(int length) {
        return new LengthBatchWindow(Collections.singletonMap("length", length));
    }

    public static Window lengthBatch(Variable<Integer> length) {
        return new LengthBatchWindow(Collections.singletonMap("length", length));
    }

    public static Window externalTimeBatch(String timestampKey, Duration duration) {
        return new ExternalTimeBatchWindow(Commons.map("timestampKey", timestampKey, "duration", duration.toMillis()));
    }

    public abstract void process(Event event, Processor nextProcessor);

    @Override
    public abstract Window copy();

    public Object getProperty(String attribute, int index) {
        Object value = this.properties.get(attribute);
        if (value == null) {
            value = this.properties.get(String.format("_param_%d", index));
            if (value == null) {
                throw new WisdomAppValidationException("%s requires %s but not available", this.getClass().getSimpleName(), attribute);
            }
        }
        return value;
    }
}
