package com.javahelps.wisdom.core.stream;

import com.javahelps.wisdom.core.event.Event;
import com.javahelps.wisdom.core.processor.Processor;

/**
 * Created by gobinath on 6/29/17.
 */
public class InputHandler {

    private final Processor processor;

    public InputHandler(Processor processor) {
        this.processor = processor;
    }

    public void send(Event event) {
        this.processor.process(event);
    }
}
