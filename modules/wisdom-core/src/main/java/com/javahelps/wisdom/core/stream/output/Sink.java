package com.javahelps.wisdom.core.stream.output;

import com.javahelps.wisdom.core.WisdomApp;
import com.javahelps.wisdom.core.event.Event;
import com.javahelps.wisdom.core.extension.ImportsManager;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public abstract class Sink {

    static {
        ImportsManager.INSTANCE.use(ConsoleSink.class);
    }

    public Sink(Map<String, ?> properties) {

    }

    public static Sink create(String namespace, Map<String, ?> properties) {
        return ImportsManager.INSTANCE.createSink(namespace, properties);
    }

    public abstract void start();

    public abstract void init(WisdomApp wisdomApp, String streamId);

    public abstract void publish(List<Event> events) throws IOException;

    public abstract void stop();
}
