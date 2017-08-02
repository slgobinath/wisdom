package com.javahelps.wisdom.core.processor;

import com.javahelps.wisdom.core.event.Event;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.function.Function;

/**
 * {@link StreamProcessor} to modify or map into a new {@link Event} based on a {@link Function}.
 */
public class AggregateProcessor extends StreamProcessor {

    private Function<List<Event>, Event> function;

    public AggregateProcessor(String id, Function<List<Event>, Event> function) {
        super(id);
        this.function = function;
    }

    @Override
    public void start() {

    }

    @Override
    public void process(Event event) {
        this.getNextProcessor().process(this.function.apply(Arrays.asList(event)));
    }

    @Override
    public void process(List<Event> events) {
        this.getNextProcessor().process(this.function.apply(events));
    }

    @Override
    public Object clone() {

        AggregateProcessor mapProcessor = new AggregateProcessor(this.id, this.function);
        mapProcessor.setNextProcessor((Processor) this.getNextProcessor().clone());
        return mapProcessor;
    }
}
