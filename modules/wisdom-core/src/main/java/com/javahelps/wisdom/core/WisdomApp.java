package com.javahelps.wisdom.core;

import com.javahelps.wisdom.core.event.Event;
import com.javahelps.wisdom.core.exception.ExceptionListener;
import com.javahelps.wisdom.core.processor.Processor;
import com.javahelps.wisdom.core.processor.StreamProcessor;
import com.javahelps.wisdom.core.query.Query;
import com.javahelps.wisdom.core.stream.InputHandler;
import com.javahelps.wisdom.core.stream.Stream;
import com.javahelps.wisdom.core.stream.StreamCallback;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * {@link WisdomApp} is the event processing application which lets the users to create stream processing components
 * and queries, and accepts the inputs.
 */
public class WisdomApp {

    private final Map<String, Stream> streamMap = new HashMap<>();
    private final Map<String, StreamProcessor> streamProcessorMap = new HashMap<>();
    private final Map<String, Query> queryMap = new HashMap<>();
    private final Map<Class<? extends Exception>, ExceptionListener> exceptionListenerMap = new HashMap<>();
    private WisdomContext wisdomContext;

    public WisdomApp() {
        this.wisdomContext = new WisdomContext();
    }

    public WisdomContext getWisdomContext() {
        return wisdomContext;
    }

    public void start() {
        this.streamProcessorMap.values().forEach(processor -> processor.init(this));
        this.streamMap.values().forEach(Processor::start);
    }

    public void shutdown() {
        this.wisdomContext.shutdown();
    }

    public Stream defineStream(String id) {
        Stream stream = new Stream(this, id);
        this.streamMap.put(id, stream);
        return stream;
    }

    public Query defineQuery(String id) {
        Query query = new Query(this, id);
        this.queryMap.put(id, query);
        return query;
    }

    public void addCallback(String streamId, StreamCallback callback) {
        Stream stream = this.streamMap.get(streamId);
        if (stream != null) {
            stream.addProcessor(new Processor() {

                @Override
                public void start() {

                }

                @Override
                public void process(Event event) {
                    callback.receive(event);
                }

                @Override
                public void process(List<Event> events) {
                    callback.receive(events.toArray(new Event[0]));
                }

                @Override
                public Object clone() {
                    return this;
                }
            });
        }
    }

    public InputHandler getInputHandler(String streamId) {
        return new InputHandler(this.getStream(streamId));
    }

    public void addExceptionListener(Class<? extends Exception> exception, ExceptionListener exceptionListener) {
        this.exceptionListenerMap.put(exception, exceptionListener);
    }

    public Stream getStream(String id) {
        return this.streamMap.get(id);
    }

    public void send(String streamId, Event event) {
        Stream stream = this.streamMap.get(streamId);
        if (stream != null) {
            stream.process(event);
        }
    }

    public void handleException(Exception exception) {
        ExceptionListener listener = this.exceptionListenerMap.get(exception.getClass());
        if (listener != null) {
            listener.onException(exception);
        } else {
            // Dump the exception
            // For debugging purposes only, throw the exception
            throw new RuntimeException(exception);
        }
    }

    public void addStreamProcessor(StreamProcessor processor) {
        this.streamProcessorMap.put(processor.getId(), processor);
    }
}
