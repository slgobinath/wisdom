package com.javahelps.wisdom.core.event;

import com.javahelps.wisdom.core.stream.Stream;

import java.util.HashMap;
import java.util.Map;

/**
 * This is the representation of any events passing through the {@link Stream}s and
 * {@link com.javahelps.wisdom.core.processor.StreamProcessor}s.
 */
public class Event {

    private long timestamp = -1;
    private Stream stream;
    private String name;
    private Map<String, Comparable> data;
    private Map<String, String> alias;
    private boolean isExpired = false;
    private Event original;

    public Event(Stream stream, long timestamp) {

        this(timestamp);
        this.stream = stream;
    }

    public Event(long timestamp) {
        this.timestamp = timestamp;
        this.data = new HashMap<>();
        this.alias = new HashMap<>();
        this.original = this;
    }

    public Attribute attribute(String attribute) {
        return new Attribute(this, attribute);
    }

    public Event set(String attribute, Comparable value) {
        this.data.put(attribute, value);
        return this;
    }

    public Comparable get(String attribute) {
        if (this.name != null) {
            if (!attribute.contains(".")) {
                attribute = this.name + "." + attribute;
            }
        }
        Comparable data = this.data.get(attribute);
        if (data == null) {
            data = this.data.get(this.alias.get(attribute));
        }
        return data;
    }

    public Event remove(String attribute) {
        this.data.remove(attribute);
        return this;
    }

    public Event rename(String attribute, String newAttribute) {
        Comparable value = this.get(attribute);
        this.remove(attribute);
        this.set(newAttribute, value);
        return this;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setExpired(boolean expired) {
        isExpired = expired;
    }

    public Stream getStream() {
        return stream;
    }

    public void setStream(Stream stream) {
        this.stream = stream;
    }

    public Event getOriginal() {
        return original;
    }

    public void setOriginal(Event original) {
        this.original = original;
    }

    public Map<String, Comparable> getData() {
        return data;
    }

    public Event copyEvent() {
        Event event = new Event(this.stream, this.timestamp);
        event.data = new HashMap<>(this.data);
        event.isExpired = this.isExpired;
        event.original = this;
        return event;
    }

    public void setAlias(String key, String as) {
        this.alias.put(as, key);
    }

    @Override
    public String toString() {
        return "Event{" +
                "timestamp=" + timestamp +
                ", stream=" + stream.getId() +
                ", data=" + data +
                ", isExpired=" + isExpired +
                '}';
    }
}
