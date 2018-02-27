package com.javahelps.wisdom.core.window;

import com.javahelps.wisdom.core.event.Event;
import com.javahelps.wisdom.core.partition.Partitionable;
import com.javahelps.wisdom.core.processor.Processor;
import com.javahelps.wisdom.core.processor.Stateful;
import com.javahelps.wisdom.core.variable.Variable;

import java.time.Duration;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * A utility to construct Windows.
 */
public abstract class Window implements Partitionable, Stateful {

    protected final Lock lock = new ReentrantLock();

    protected Window() {

    }

    public static Window length(int length) {
        return new LengthWindow(length);
    }

    public static Window length(Variable<Integer> length) {
        return new LengthWindow(length);
    }

    public static Window lengthBatch(int length) {
        return new LengthBatchWindow(length);
    }

    public static Window lengthBatch(Variable<Integer> length) {
        return new LengthBatchWindow(length);
    }

    public static Window externalTimeBatch(String timestampKey, Duration duration) {
        return new ExternalTimeBatchWindow(timestampKey, duration);
    }

    public abstract void process(Event event, Processor nextProcessor);

    @Override
    public abstract Window copy();

}
