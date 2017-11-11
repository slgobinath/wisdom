package com.javahelps.wisdom.core.processor;

import com.javahelps.wisdom.core.WisdomApp;
import com.javahelps.wisdom.core.stream.Stream;

/**
 * A {@link Processor} that comes in a {@link com.javahelps.wisdom.core.query.Query} following {@link Stream}.
 * In technical aspect, a {@link com.javahelps.wisdom.core.query.Query} is a linked list of these processors.
 */
public abstract class StreamProcessor implements Processor {

    protected String id;
    private Processor previousProcessor;
    private Processor nextProcessor;

    public StreamProcessor(String id) {
        this.id = id;
    }

    public String getId() {
        return id;
    }

    public Processor getNextProcessor() {
        return nextProcessor;
    }

    public void setNextProcessor(Processor nextProcessor) {
        this.nextProcessor = nextProcessor;
    }

    public void init(WisdomApp wisdomApp) {

    }

    @Override
    public void stop() {
        // Do nothing
    }
}
