package com.javahelps.wisdom.core.processor;

import com.javahelps.wisdom.core.event.Event;
import com.javahelps.wisdom.core.stream.Stream;

import java.util.Collection;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * {@link StreamProcessor} to modify or map into a new {@link Event} based on a {@link Function}.
 */
public class MapProcessor extends StreamProcessor {

    private Function<Event, Event> function;

    public MapProcessor(String id, Stream inputStream, Function<Event, Event> function) {
        super(id);
        this.function = function;
    }

    @Override
    public void start() {

    }

    @Override
    public void process(Event event) {
        this.getNextProcessor().process(this.function.apply(event));
    }

    @Override
    public void process(Collection<Event> events) {
        Collection<Event> eventsAfterMapping = events.stream().map(this.function).collect(Collectors.toList());
        this.getNextProcessor().process(eventsAfterMapping);
    }
}
