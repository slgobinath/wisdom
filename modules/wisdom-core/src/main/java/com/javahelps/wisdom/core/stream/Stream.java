package com.javahelps.wisdom.core.stream;

import com.javahelps.wisdom.core.WisdomApp;
import com.javahelps.wisdom.core.event.Event;
import com.javahelps.wisdom.core.exception.WisdomAppRuntimeException;
import com.javahelps.wisdom.core.processor.Processor;
import com.javahelps.wisdom.core.stream.async.EventHolder;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.YieldingWaitStrategy;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import static com.javahelps.wisdom.core.util.WisdomConstants.ASYNC;
import static com.javahelps.wisdom.core.util.WisdomConstants.BUFFER;

/**
 * {@link Stream} is the fundamental data-structure of the event processor. At the runtime, it acts newName the entry point
 * and manager of all the queries starting with this newName the input stream.
 */
public class Stream implements Processor {

    private static final Logger LOGGER = LoggerFactory.getLogger(Stream.class);

    protected String id;
    private WisdomApp wisdomApp;
    private List<Processor> processorList = new ArrayList<>();
    private Processor[] processors;
    private int noOfProcessors;
    private Disruptor<EventHolder> disruptor;
    private RingBuffer<EventHolder> ringBuffer;
    private boolean disabled = false;


    public Stream(WisdomApp wisdomApp, String id) {
        this(wisdomApp, id, new Properties());
    }

    public Stream(WisdomApp wisdomApp, String id, Properties properties) {
        this.id = id;
        this.wisdomApp = wisdomApp;
        final boolean async = ((Boolean) properties.getOrDefault(ASYNC, wisdomApp.isAsync()));
        final int bufferSize = ((Number) properties.getOrDefault(BUFFER, wisdomApp.getBufferSize())).intValue();

        // Create disruptor if async mode is enables
        if (async) {
            this.disruptor = new Disruptor<>(EventHolder::new, bufferSize,
                    wisdomApp.getWisdomContext().getThreadFactory(),
                    ProducerType.MULTI, new YieldingWaitStrategy());

            // Connect the handler
            disruptor.handleEventsWith((eventHolder, sequence, endOfBatch) -> this.sendToProcessors(eventHolder.get()));

            // Get the ring buffer from the Disruptor to be used for publishing.
            this.ringBuffer = disruptor.getRingBuffer();
        }
    }

    @Override
    public void start() {
        this.noOfProcessors = this.processorList.size();
        this.processors = this.processorList.toArray(new Processor[0]);

        if (this.disruptor != null) {
            // Start the Disruptor, starts all threads running
            disruptor.start();
        }
    }

    @Override
    public void stop() {
        if (this.disruptor != null) {
            // Start the Disruptor, starts all threads running
            disruptor.shutdown();
        }
    }

    @Override
    public void process(Event event) {

        if (this.disabled) {
            return;
        }
        if (this.disruptor == null) {
            this.sendToProcessors(event);
        } else {
            // Async enabled
            this.ringBuffer.publishEvent((eventHolder, sequence, buffer) -> eventHolder.set(event));
        }
    }

    @Override
    public void process(List<Event> events) {

        if (this.disabled) {
            return;
        }
        if (this.noOfProcessors == 1) {
            this.processors[0].process(events);
        } else {
            for (Processor processor : this.processors) {
                List<Event> newEvents = this.convertEvent(events);
                processor.process(newEvents);
            }
        }
    }

    private void sendToProcessors(Event event) {
        Event newEvent = this.convertEvent(event);
        if (this.noOfProcessors == 1) {
            this.processors[0].process(newEvent);
        } else {
            for (Processor processor : this.processors) {
                try {
                    processor.process(newEvent);
                } catch (WisdomAppRuntimeException ex) {
                    this.wisdomApp.handleException(ex);
                }
            }
        }
    }

    public String getId() {
        return id;
    }

    private Event convertEvent(Event from) {

        Event newEvent = from.copyEvent();
        newEvent.setStream(this);
        return newEvent;
    }

    private List<Event> convertEvent(List<Event> from) {

        List<Event> newEvents = new ArrayList<>(from.size());
        for (Event event : from) {
            newEvents.add(convertEvent(event));
        }
        return newEvents;
    }

    public void addProcessor(Processor processor) {
        if (!this.processorList.contains(processor)) {
            this.processorList.add(processor);
        }
    }

    public void addProcessor(Processor processor, int index) {
        if (!this.processorList.contains(processor)) {
            this.processorList.add(index, processor);
        }
    }

    public void removeProcessor(Processor processor) {
        this.processorList.remove(processor);
    }

    @Override
    public Processor copy() {

        return this;
    }

    public void enable() {
        this.disabled = false;
    }

    public void disable() {
        this.disabled = true;
    }
}
