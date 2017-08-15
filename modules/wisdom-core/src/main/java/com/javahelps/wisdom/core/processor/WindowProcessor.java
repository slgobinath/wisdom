package com.javahelps.wisdom.core.processor;

import com.javahelps.wisdom.core.event.Event;
import com.javahelps.wisdom.core.window.Window;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * The runtime {@link StreamProcessor} of {@link Window}s.
 */
public class WindowProcessor extends StreamProcessor {


    private Window window;

    public WindowProcessor(String id, Window window) {
        super(id);
        this.window = window;
    }

    @Override
    public void start() {

    }

    @Override
    public void process(Event event) {

        this.window.process(event, getNextProcessor());
    }

    @Override
    public void process(List<Event> events) {

    }

    @Override
    public Object clone() {

        WindowProcessor windowProcessor = new WindowProcessor(this.id, this.window);
        windowProcessor.setNextProcessor((Processor) this.getNextProcessor().clone());
        return windowProcessor;
    }
}
