package com.javahelps.wisdom.core.stream.output;

import com.javahelps.wisdom.core.event.Event;

import java.util.List;

public class ConsoleSink implements Sink {

    @Override
    public void start() {

    }

    @Override
    public void publish(List<Event> events) {
        System.out.println(events);
    }

    @Override
    public void stop() {

    }
}
