package com.javahelps.wisdom.core.stream.output;

import com.javahelps.wisdom.core.event.Event;

import java.io.IOException;
import java.util.List;

public interface Sink {

    void start();

    void publish(List<Event> events) throws IOException;

    void stop();
}
