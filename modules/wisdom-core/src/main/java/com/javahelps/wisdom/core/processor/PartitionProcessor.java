package com.javahelps.wisdom.core.processor;

import com.javahelps.wisdom.core.WisdomApp;
import com.javahelps.wisdom.core.event.Event;
import com.javahelps.wisdom.core.exception.WisdomAppValidationException;
import com.javahelps.wisdom.core.stream.async.EventHolder;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.YieldingWaitStrategy;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class PartitionProcessor extends StreamProcessor implements Stateful {

    private final String[] attributes;
    private final Map<String, Processor> processorMap = new HashMap<>();
    private final Lock lock = new ReentrantLock();
    private Disruptor<EventHolder> disruptor;
    private RingBuffer<EventHolder> ringBuffer;

    public PartitionProcessor(String id, String... attributes) {
        super(id);
        if (attributes.length == 0) {
            throw new WisdomAppValidationException("Partition require at least an attribute but received nothing");
        }
        this.attributes = attributes;
    }

    @Override
    public void init(WisdomApp wisdomApp) {

        if (wisdomApp.getContext().isAsync()) {
            this.disruptor = new Disruptor<>(EventHolder::new, wisdomApp.getBufferSize(),
                    wisdomApp.getContext().getThreadFactory(),
                    ProducerType.SINGLE, new YieldingWaitStrategy());

            // Connect the handler
            disruptor.handleEventsWith((eventHolder, sequence, endOfBatch) -> this.sendToPartition(eventHolder.get()));

            // Get the ring buffer from the Disruptor to be used for publishing.
            this.ringBuffer = disruptor.getRingBuffer();
        }
    }

    @Override
    public void start() {
        if (this.disruptor != null) {
            this.disruptor.start();
        }
    }

    @Override
    public void stop() {
        if (this.disruptor != null) {
            this.disruptor.shutdown();
        }
    }

    @Override
    public void process(Event event) {

        if (this.disruptor != null) {
            this.ringBuffer.publishEvent((eventHolder, sequence, buffer) -> eventHolder.set(event));
        } else {
            this.sendToPartition(event);
        }
    }

    @Override
    public void process(List<Event> events) {
        for (Event event : events) {
            this.process(event);
        }
    }

    private void sendToPartition(Event event) {
        this.getNexProcessor(event).process(event);
    }

    private Processor getNexProcessor(Event event) {
        String key = this.calculateKey(event);
        try {
            this.lock.lock();
            Processor nextProcessor = this.processorMap.get(key);
            if (nextProcessor == null) {
                nextProcessor = getNextProcessor().copy();
                this.processorMap.putIfAbsent(key, nextProcessor);
            }
            return nextProcessor;
        } finally {
            this.lock.unlock();
        }
    }

    private String calculateKey(Event event) {
        if (this.attributes.length == 1) {
            return Objects.toString(event.get(this.attributes[0]));
        } else {
            StringBuilder builder = new StringBuilder();
            for (String attribute : this.attributes) {
                builder.append(Objects.toString(event.get(attribute)));
            }
            return builder.toString();
        }

    }

    @Override
    public Processor copy() {
        return null;
    }

    @Override
    public void destroy() {
        this.clear();
    }

    @Override
    public void clear() {
        try {
            this.lock.lock();
            for (Processor processor : this.processorMap.values()) {
                processor.destroy();
            }
            this.processorMap.clear();
        } finally {
            this.lock.unlock();
        }
    }
}
