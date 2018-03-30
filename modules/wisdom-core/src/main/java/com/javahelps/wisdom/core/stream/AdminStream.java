package com.javahelps.wisdom.core.stream;

import com.javahelps.wisdom.core.WisdomApp;
import com.javahelps.wisdom.core.event.Event;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.function.Consumer;

public class AdminStream extends Stream {

    private static final Logger LOGGER = LoggerFactory.getLogger(AdminStream.class);

    private final Map<String, Set<Consumer<Map<String, Comparable>>>> consumers = new HashMap<>();

    public AdminStream(WisdomApp wisdomApp, String id) {
        super(wisdomApp, id);
    }

    public AdminStream(WisdomApp wisdomApp, String id, Properties properties) {
        super(wisdomApp, id, properties);
    }

    @Override
    public void process(Event event) {
        super.process(event);
        this.execute(event);
    }

    @Override
    public void process(List<Event> events) {
        super.process(events);
        events.forEach(this::execute);
    }

    private void execute(Event event) {
        String command = (String) event.get("command");
        if (command != null) {
            // Command is case sensitive
            Set<Consumer<Map<String, Comparable>>> set = this.consumers.get(command);
            if (set != null) {
                Map<String, Comparable> data = Collections.unmodifiableMap(event.getData());
                for (Consumer<Map<String, Comparable>> consumer : set) {
                    try {
                        consumer.accept(data);
                    } catch (Exception ex) {
                        LOGGER.error("Error in consumer", ex);
                    }
                }
            }
        }
    }

    public void register(String command, Consumer<Map<String, Comparable>> consumer) {
        Objects.requireNonNull(command, "Command cannot be null");
        Objects.requireNonNull(consumer, "Consumer cannot be null");
        Set<Consumer<Map<String, Comparable>>> set = this.consumers.get(command);
        if (set == null) {
            set = new LinkedHashSet<>();
            this.consumers.put(command, set);
        }
        set.add(consumer);
    }

    public void unregister(String command, Consumer<Map<String, Comparable>> consumer) {
        Objects.requireNonNull(command, "Command cannot be null");
        Objects.requireNonNull(consumer, "Consumer cannot be null");
        Set<Consumer<Map<String, Comparable>>> set = this.consumers.get(command);
        if (set != null) {
            set.remove(consumer);
        }
    }
}
