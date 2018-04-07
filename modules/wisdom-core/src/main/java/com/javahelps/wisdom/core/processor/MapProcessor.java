package com.javahelps.wisdom.core.processor;

import com.javahelps.wisdom.core.event.Event;

import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * {@link StreamProcessor} to modify or map into a new {@link Event} based on a {@link Function}.
 */
public class MapProcessor extends StreamProcessor {

    private Function<Event, Event> function;

    public MapProcessor(String id, Function<Event, Event> function) {
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
    public void process(List<Event> events) {
        List<Event> eventsAfterMapping = events.stream().map(this.function).collect(Collectors.toList());
        this.getNextProcessor().process(eventsAfterMapping);
    }

    @Override
    public Processor copy() {

        MapProcessor mapProcessor = new MapProcessor(this.id, this.function);
        mapProcessor.setNextProcessor(this.getNextProcessor().copy());
        return mapProcessor;
    }

    @Override
    public void destroy() {
        this.function = null;
    }
}
