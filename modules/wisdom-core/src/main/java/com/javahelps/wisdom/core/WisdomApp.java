package com.javahelps.wisdom.core;

import com.javahelps.wisdom.core.event.Event;
import com.javahelps.wisdom.core.exception.ExceptionListener;
import com.javahelps.wisdom.core.exception.WisdomAppValidationException;
import com.javahelps.wisdom.core.processor.Processor;
import com.javahelps.wisdom.core.processor.Stateful;
import com.javahelps.wisdom.core.query.Query;
import com.javahelps.wisdom.core.stream.InputHandler;
import com.javahelps.wisdom.core.stream.Stream;
import com.javahelps.wisdom.core.stream.StreamCallback;
import com.javahelps.wisdom.core.stream.output.Sink;
import com.javahelps.wisdom.core.util.WisdomConfig;
import com.javahelps.wisdom.core.variable.Variable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

import static com.javahelps.wisdom.core.util.WisdomConfig.WISDOM_APP_NAME;
import static com.javahelps.wisdom.core.util.WisdomConfig.WISDOM_APP_VERSION;
import static com.javahelps.wisdom.core.util.WisdomConstants.*;

/**
 * {@link WisdomApp} is the event processing application which lets the users to create stream processing components
 * and queries, and accepts the inputs.
 */
public class WisdomApp implements Stateful {

    private static final Logger LOGGER = LoggerFactory.getLogger(WisdomApp.class);
    private final String name;
    private final String version;

    private final int bufferSize;
    private final Map<String, Stream> streamMap = new HashMap<>();
    private final Map<String, Variable> variableMap = new HashMap<>();
    private final Map<String, Query> queryMap = new HashMap<>();
    private final Map<Class<? extends Exception>, ExceptionListener> exceptionListenerMap = new HashMap<>();
    private final WisdomContext wisdomContext;
    private final ThreadBarrier threadBarrier;

    public WisdomApp() {
        this(WISDOM_APP_NAME, WISDOM_APP_VERSION);
    }

    public WisdomApp(String name, String version) {
        this.name = name;
        this.version = version;
        this.bufferSize = WisdomConfig.EVENT_BUFFER_SIZE;
        this.wisdomContext = new WisdomContext(new Properties());
        this.threadBarrier = this.wisdomContext.getThreadBarrier();
    }

    public WisdomApp(Properties properties) {
        this.name = properties.getProperty(NAME, WISDOM_APP_NAME);
        this.version = properties.getProperty(VERSION, WISDOM_APP_VERSION);
        this.bufferSize = ((Number) properties.getOrDefault(BUFFER, WisdomConfig.EVENT_BUFFER_SIZE)).intValue();
        this.wisdomContext = new WisdomContext(properties);
        this.threadBarrier = this.wisdomContext.getThreadBarrier();
    }

    public WisdomContext getContext() {
        return wisdomContext;
    }

    public void start() {
        this.wisdomContext.init(this);
        this.wisdomContext.start();
        this.queryMap.values().forEach(Query::init);
        this.streamMap.values().forEach(Processor::start);
    }

    public void shutdown() {
        // Stop all streams
        for (Stream stream : this.streamMap.values()) {
            stream.stop();
        }
        this.wisdomContext.shutdown();
    }

    public Stream defineStream(String id) {
        Stream stream = new Stream(this, id);
        this.streamMap.put(id, stream);
        return stream;
    }

    public Stream defineStream(String id, Properties properties) {
        Stream stream = new Stream(this, id, properties);
        this.streamMap.put(id, stream);
        return stream;
    }

    public <T> Variable<T> defineVariable(String id, T defaultValue) {
        Variable<T> variable = new Variable(id, defaultValue);
        this.variableMap.put(id, variable);
        return variable;
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
                public void stop() {

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
                public Processor copy() {
                    return this;
                }
            }, 0);
        }
    }

    /**
     * Create input handler to feed events to the given stream.
     *
     * @param streamId the Wisdom stream id
     * @return new {@link InputHandler}
     */
    public InputHandler getInputHandler(String streamId) {

        Stream stream = this.streamMap.get(streamId);
        if (stream == null) {
            throw new WisdomAppValidationException("Stream id %s is not defined", streamId);
        }
        return new InputHandler(stream, this);
    }

    public void addExceptionListener(Class<? extends Exception> exception, ExceptionListener exceptionListener) {
        this.exceptionListenerMap.put(exception, exceptionListener);
    }

    public Stream getStream(String id) {
        return this.streamMap.get(id);
    }

    public Variable getVariable(String id) {
        return this.variableMap.get(id);
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
            LOGGER.error("Exception in " + this.name, exception);
        }
    }

    public void addSink(String streamId, Sink sink) {
        Stream stream = this.streamMap.get(streamId);
        if (stream == null) {
            throw new WisdomAppValidationException("Stream id %s is not defined", streamId);
        }
        sink.init(this, streamId);
        stream.addProcessor(new Processor() {
            @Override
            public void start() {
                sink.start();
            }

            @Override
            public void stop() {
                sink.stop();
            }

            @Override
            public void process(Event event) {
                try {
                    sink.publish(Arrays.asList(event));
                } catch (Exception e) {
                    WisdomApp.this.handleException(e);
                }
            }

            @Override
            public void process(List<Event> events) {
                try {
                    sink.publish(events);
                } catch (Exception e) {
                    WisdomApp.this.handleException(e);
                }
            }

            @Override
            public Processor copy() {
                return this;
            }
        });
    }

    public String getName() {
        return name;
    }

    public String getVersion() {
        return version;
    }

    public int getBufferSize() {
        return bufferSize;
    }

    @Override
    public void clear() {
        this.threadBarrier.lock();
        this.streamMap.values().forEach(Stream::disable);
        this.queryMap.values().forEach(Query::clear);
        this.streamMap.values().forEach(Stream::enable);
        this.threadBarrier.unlock();
    }
}
