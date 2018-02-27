package com.javahelps.wisdom.core.event;

import com.javahelps.wisdom.core.exception.AttributeNotFoundException;
import com.javahelps.wisdom.core.exception.WisdomAppRuntimeException;
import com.javahelps.wisdom.core.stream.Stream;

import java.util.HashMap;
import java.util.Map;

/**
 * This is the representation of any events passing through the {@link Stream}s and
 * {@link com.javahelps.wisdom.core.processor.StreamProcessor}s.
 */
public class Event {

    public static final Index FIRST = new Index(0);
    public static final Index LAST = new Index(Integer.MAX_VALUE);

    private long timestamp = -1;
    private Stream stream;
    private String name;
    private Map<String, Comparable> data;
    private transient Map<String, String> alias;
    private boolean expired = false;
    private boolean reset = false;
    private transient Event original;

    public Event(Stream stream, long timestamp) {

        this(timestamp);
        this.stream = stream;
    }

    public Event(long timestamp) {
        this.timestamp = timestamp;
        this.data = new HashMap<>();
        this.alias = new HashMap<>();
        this.original = null;
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

    public Number getAsNumber(String attribute) {
        Comparable value = this.get(attribute);
        if (value == null) {
            throw new AttributeNotFoundException(String.format("Attribute %s not found in event %s", attribute,
                    this.toString()));
        }
        if (!(value instanceof Number)) {
            throw new WisdomAppRuntimeException(String.format("Cannot convert attribute %s from %s to java.lang" +
                    ".Number", attribute, value.getClass().getSimpleName()));
        }
        return ((Number) value);
    }

    public long getAsLong(String attribute) {
        return this.getAsNumber(attribute).longValue();
    }

    public double getAsDouble(String attribute) {
        return this.getAsNumber(attribute).doubleValue();
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
        this.expired = expired;
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

    public boolean isReset() {
        return reset;
    }

    public void setReset(boolean reset) {
        this.reset = reset;
    }

    public Event copyEvent() {
        Event event = new Event(this.stream, this.timestamp);
        event.data = new HashMap<>(this.data);
        event.expired = this.expired;
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
                ", stream=" + (stream == null ? "" : stream.getId()) +
                ", data=" + data +
                ", expired=" + expired +
                '}';
    }
}
