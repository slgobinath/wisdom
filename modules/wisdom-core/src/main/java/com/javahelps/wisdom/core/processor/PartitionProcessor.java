package com.javahelps.wisdom.core.processor;

import com.javahelps.wisdom.core.event.Event;
import com.javahelps.wisdom.core.exception.WisdomAppValidationException;

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

    public PartitionProcessor(String id, String... attributes) {
        super(id);
        if (attributes.length == 0) {
            throw new WisdomAppValidationException("Partition require at least an attribute but received nothing");
        }
        this.attributes = attributes;
    }

    @Override
    public void start() {

    }

    @Override
    public void process(Event event) {

        this.getNexProcessor(event).process(event);
    }

    @Override
    public void process(List<Event> events) {

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
    public void clear() {
        try {
            this.lock.lock();
            this.processorMap.clear();
        } finally {
            this.lock.unlock();
        }
    }
}
