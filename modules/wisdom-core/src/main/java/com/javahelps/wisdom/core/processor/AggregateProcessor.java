package com.javahelps.wisdom.core.processor;

import com.javahelps.wisdom.core.event.Event;
import com.javahelps.wisdom.core.util.EventGenerator;

import java.util.List;
import java.util.function.Function;

/**
 * {@link StreamProcessor} to modify or map into a new {@link Event} based on a {@link Function}.
 */
public class AggregateProcessor extends StreamProcessor {

    private Function<Event, Event> function;

    public AggregateProcessor(String id, Function<Event, Event> function) {
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
        int length = events.size();
        if (length > 0) {
            for (int i = 0; i < length; i++) {
                events.set(i, this.function.apply(events.get(i)));
            }
            // Reset the function
            this.function.apply(EventGenerator.getResetEvent());
            this.getNextProcessor().process(events.get(length - 1));
        }
    }

    @Override
    public Processor copy() {

        AggregateProcessor mapProcessor = new AggregateProcessor(this.id, this.function);
        mapProcessor.setNextProcessor(this.getNextProcessor().copy());
        return mapProcessor;
    }
}
