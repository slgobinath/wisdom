package com.javahelps.wisdom.core.processor;

import com.javahelps.wisdom.core.stream.Stream;

/**
 * A {@link Processor} that comes in a {@link com.javahelps.wisdom.core.query.Query} following {@link Stream}.
 * In technical aspect, a {@link com.javahelps.wisdom.core.query.Query} is a linked list of these processors.
 */
public abstract class StreamProcessor implements Processor {

    private String id;
    private Stream inputStream;
    private Processor previousProcessor;
    private Processor nextProcessor;

    public StreamProcessor(String id, Stream inputStream) {
        this.id = id;
        this.inputStream = inputStream;
    }

    public String getId() {
        return id;
    }

    public Stream getInputStream() {
        return inputStream;
    }

    public void setNextProcessor(Processor nextProcessor) {
        this.nextProcessor = nextProcessor;
    }

    public Processor getNextProcessor() {
        return nextProcessor;
    }
}
