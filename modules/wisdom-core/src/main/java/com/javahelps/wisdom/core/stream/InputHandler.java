package com.javahelps.wisdom.core.stream;

import com.javahelps.wisdom.core.WisdomApp;
import com.javahelps.wisdom.core.event.Event;
import com.javahelps.wisdom.core.exception.WisdomAppRuntimeException;
import com.javahelps.wisdom.core.processor.Processor;

/**
 * Created by gobinath on 6/29/17.
 */
public class InputHandler {

    private final Processor processor;
    private final WisdomApp wisdomApp;

    public InputHandler(Processor processor, WisdomApp wisdomApp) {
        this.processor = processor;
        this.wisdomApp = wisdomApp;
    }

    public void send(Event event) {
        try {
            this.processor.process(event);
        } catch (WisdomAppRuntimeException ex) {
            this.wisdomApp.handleException(ex);
        }
    }
}
