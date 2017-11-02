package com.javahelps.wisdom.core.variable;

import com.javahelps.wisdom.core.event.Event;
import com.javahelps.wisdom.core.processor.Processor;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Wisdom variable which can be updated using a stream and used to dynamically
 * change the properties of other components at the runtime.
 */
public class Variable<T> implements Processor {

    private final String id;
    private T value;
    private final ReadWriteLock lock;
    private final List<OnUpdateListener<T>> listeners;

    public Variable(String id, T value) {
        this.id = id;
        this.lock = new ReentrantReadWriteLock();
        this.listeners = new ArrayList<>();
        this.value = value;
    }

    /**
     * Get the current value of the variable.
     *
     * @return the current value
     */
    public T get() {
        this.lock.readLock().lock();
        try {
            return this.value;
        } finally {
            this.lock.readLock().unlock();
        }
    }

    /**
     * Set a new value to the variable.
     *
     * @param value the new value
     */
    public void set(T value) {
        this.lock.writeLock().lock();
        try {
            this.value = value;
        } finally {
            this.lock.writeLock().unlock();
        }
        // Notify the listeners
        for (OnUpdateListener<T> listener : this.listeners) {
            listener.update(value);
        }
    }

    /**
     * Add a new listener to listen for any updates to the variable.
     *
     * @param listener the listener object
     */
    public void addOnUpdateListener(OnUpdateListener listener) {
        synchronized (this) {
            this.listeners.add(listener);
        }
    }

    /**
     * Remove the listener from listening for any updates to the variable.
     *
     * @param listener
     */
    public void removeOnUpdateListener(OnUpdateListener listener) {
        synchronized (this) {
            this.listeners.remove(listener);
        }
    }

    @Override
    public void start() {
        // Do nothing
    }

    @Override
    public void process(Event event) {
        T value = (T) event.get(this.id);
        if (value != null) {
            this.set(value);
        }
    }

    @Override
    public void process(List<Event> events) {
        // Update from the last event
        this.process(events.get(events.size() - 1));
    }

    @Override
    public Processor copy() {
        return null;
    }

    @FunctionalInterface
    public interface OnUpdateListener<T> {
        void update(T value);
    }

}
