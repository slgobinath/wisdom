package com.javahelps.wisdom.core.processor;

import com.javahelps.wisdom.core.event.Event;
import com.javahelps.wisdom.core.processor.StreamProcessor;
import com.javahelps.wisdom.core.stream.Stream;
import com.javahelps.wisdom.core.window.Window;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * The runtime {@link StreamProcessor} of {@link Window}s.
 */
public class WindowProcessor extends StreamProcessor {

    private List<Event> events;
    private Window window;

    public WindowProcessor(String id, Stream inputStream, Window window) {
        super(id, inputStream);
        this.events = new ArrayList<>();
        this.window = window;
    }

    @Override
    public void process(Event event) {

        this.window.process(this.events, event, getNextProcessor());
    }

    @Override
    public void process(Collection<Event> events) {

    }
}
