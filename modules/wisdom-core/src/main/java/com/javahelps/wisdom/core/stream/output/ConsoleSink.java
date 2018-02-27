package com.javahelps.wisdom.core.stream.output;

import com.javahelps.wisdom.core.WisdomApp;
import com.javahelps.wisdom.core.event.Event;

import java.util.List;

public class ConsoleSink implements Sink {

    @Override
    public void start() {

    }

    @Override
    public void init(WisdomApp wisdomApp, String streamId) {

    }

    @Override
    public void publish(List<Event> events) {
        System.out.println(events);
    }

    @Override
    public void stop() {

    }
}
