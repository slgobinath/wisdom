package com.javahelps.wisdom.core.stream.output;

import com.javahelps.wisdom.core.WisdomApp;
import com.javahelps.wisdom.core.event.Event;
import com.javahelps.wisdom.core.extension.WisdomExtension;

import java.util.Collections;
import java.util.List;
import java.util.Map;

@WisdomExtension("console")
public class ConsoleSink extends Sink {

    public ConsoleSink() {
        this(Collections.emptyMap());
    }

    public ConsoleSink(Map<String, ?> properties) {
        super(properties);

    }

    @Override
    public void start() {

    }

    @Override
    public void init(WisdomApp wisdomApp, String streamId) {

    }

    @Override
    public void publish(List<Event> events) {
        for (Event event : events) {
            System.out.println(event);
        }
    }

    @Override
    public void stop() {

    }
}
