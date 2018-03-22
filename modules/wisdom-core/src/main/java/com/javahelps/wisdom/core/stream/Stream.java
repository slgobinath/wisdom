package com.javahelps.wisdom.core.stream;

import com.javahelps.wisdom.core.ThreadBarrier;
import com.javahelps.wisdom.core.WisdomApp;
import com.javahelps.wisdom.core.event.Event;
import com.javahelps.wisdom.core.exception.WisdomAppRuntimeException;
import com.javahelps.wisdom.core.processor.Processor;
import com.javahelps.wisdom.core.statistics.StreamTracker;
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

import static com.javahelps.wisdom.core.util.WisdomConstants.*;

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
    private StreamTracker tracker;
    private ThreadBarrier threadBarrier;


    public Stream(WisdomApp wisdomApp, String id) {
        this(wisdomApp, id, new Properties());
    }

    public Stream(WisdomApp wisdomApp, String id, Properties properties) {
        this.id = id;
        this.wisdomApp = wisdomApp;
        this.threadBarrier = wisdomApp.getContext().getThreadBarrier();
        final boolean async = ((Boolean) properties.getOrDefault(ASYNC, wisdomApp.getContext().isAsync()));
        final int bufferSize = ((Number) properties.getOrDefault(BUFFER, wisdomApp.getBufferSize())).intValue();
        boolean statisticsEnabled = (boolean) properties.getOrDefault(STATISTICS, wisdomApp.getContext().isStatisticsEnabled());

        // Create disruptor if async mode is enables
        if (async) {
            this.disruptor = new Disruptor<>(EventHolder::new, bufferSize,
                    wisdomApp.getContext().getThreadFactory(),
                    ProducerType.MULTI, new YieldingWaitStrategy());

            // Connect the handler
            disruptor.handleEventsWith((eventHolder, sequence, endOfBatch) -> this.sendToProcessors(eventHolder.get()));

            // Get the ring buffer from the Disruptor to be used for publishing.
            this.ringBuffer = disruptor.getRingBuffer();
        }

        // Get stream tracker
        if (statisticsEnabled) {
            this.tracker = wisdomApp.getContext().getStatisticsManager().createStreamTracker(this.id);
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
        Event newEvent = this.convertEvent(event);
        if (this.tracker != null) {
            this.tracker.inEvent();
        }
        if (this.disruptor == null) {
            this.sendToProcessors(newEvent);
        } else {
            // Async enabled
            this.ringBuffer.publishEvent((eventHolder, sequence, buffer) -> eventHolder.set(newEvent));
        }
    }

    @Override
    public void process(List<Event> events) {

        if (this.disabled) {
            return;
        }
        List<Event> newEvents = this.convertEvent(events);
        if (this.tracker != null) {
            this.tracker.inEvent(newEvents.size());
        }
        if (this.disruptor == null) {
            for (Processor processor : this.processors) {
                try {
                    this.threadBarrier.pass();
                    processor.process(newEvents);
                } catch (WisdomAppRuntimeException ex) {
                    this.wisdomApp.handleException(ex);
                }
            }
        } else {
            // Async enabled
            for (Event event : newEvents) {
                this.ringBuffer.publishEvent((eventHolder, sequence, buffer) -> eventHolder.set(event));
            }
        }
    }

    private void sendToProcessors(Event event) {
        for (Processor processor : this.processors) {
            try {
                this.threadBarrier.pass();
                processor.process(event);
            } catch (WisdomAppRuntimeException ex) {
                this.wisdomApp.handleException(ex);
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

    public void setTracker(StreamTracker tracker) {
        this.tracker = tracker;
    }
}
