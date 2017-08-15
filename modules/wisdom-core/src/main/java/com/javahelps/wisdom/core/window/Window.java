package com.javahelps.wisdom.core.window;

import com.javahelps.wisdom.core.event.Event;
import com.javahelps.wisdom.core.partition.Partitionable;
import com.javahelps.wisdom.core.processor.Processor;

/**
 * A utility to construct Windows.
 */
public abstract class Window implements Partitionable {

    protected Window() {

    }

    public static Window length(int length) {
        return new LengthWindow(length);
    }

    public static Window lengthBatch(int length) {
        return new LengthBatchWindow(length);
    }

    public abstract void process(Event event, Processor nextProcessor);

    @Override
    public abstract Window copy();

}
