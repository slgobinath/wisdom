package com.javahelps.wisdom.core.stream.async;

import com.javahelps.wisdom.core.event.Event;

public class EventHolder {

    private Event event;

    public void set(Event event) {
        this.event = event;
    }

    public Event get() {
        return this.event;
    }
}
