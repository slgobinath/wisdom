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
    private boolean isExpired = false;

    public Event(Stream stream, long timestamp) {
        this.stream = stream;
        this.timestamp = timestamp;
        if (stream != null) {
            this.data = new HashMap<>(stream.getAttributes().length);
        } else {
            this.data = new HashMap<>();
        }
    }

    public Event(long timestamp) {
        this.timestamp = timestamp;
        this.data = new HashMap<>();
    }

    public Event set(String attribute, Comparable value) {
        this.data.put(attribute, value);
        return this;
    }

    public Comparable get(String attribute) {
        return this.data.get(attribute);
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

    public void setName(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setStream(Stream stream) {
        this.stream = stream;
    }

    public Stream getStream() {
        return stream;
    }

    public Map<String, Comparable> getData() {
        return data;
    }

    public static Attribute attribute(String attribute) {
        return new Attribute(attribute);
    }

    public Event copyEvent() {
        Event event = new Event(this.stream, this.timestamp);
        event.data = new HashMap<>(this.data);
        event.isExpired = this.isExpired;
        return event;
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
