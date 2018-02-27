package com.javahelps.wisdom.core.processor;

import com.javahelps.wisdom.core.event.Event;
import com.javahelps.wisdom.core.util.EventGenerator;

import java.util.List;
import java.util.function.Function;

/**
 * {@link StreamProcessor} to modify or map into a new {@link Event} based on a {@link Function}.
 */
public class AggregateProcessor extends StreamProcessor {

    private Function<Event, Comparable> function;
    private String setAs;

    public AggregateProcessor(String id, Function<Event, Comparable> function, String setAs) {
        super(id);
        this.function = function;
        this.setAs = setAs;
    }

    @Override
    public void start() {

    }

    @Override
    public void process(Event event) {
        event.set(this.setAs, this.function.apply(event));
        this.getNextProcessor().process(event);
    }

    @Override
    public void process(List<Event> events) {
        int lastIndex = events.size() - 1;
        if (lastIndex >= 0) {
            for (int i = 0; i < lastIndex; i++) {
                this.function.apply(events.get(i));
            }
            // Set the attribute only in last event
            Event lastEvent = events.get(lastIndex);
            lastEvent.set(this.setAs, this.function.apply(lastEvent));
            // Reset the function
            this.function.apply(EventGenerator.getResetEvent());
            this.getNextProcessor().process(lastEvent);
        }
    }

    @Override
    public Processor copy() {

        AggregateProcessor mapProcessor = new AggregateProcessor(this.id, this.function, this.setAs);
        mapProcessor.setNextProcessor(this.getNextProcessor().copy());
        return mapProcessor;
    }
}
