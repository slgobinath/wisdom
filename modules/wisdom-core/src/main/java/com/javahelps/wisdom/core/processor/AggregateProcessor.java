package com.javahelps.wisdom.core.processor;

import com.javahelps.wisdom.core.event.Event;
import com.javahelps.wisdom.core.operator.AggregateOperator;

import java.util.List;
import java.util.function.Function;

/**
 * {@link StreamProcessor} to modify or map into a new {@link Event} based on a {@link Function}.
 */
public class AggregateProcessor extends StreamProcessor {

    private AggregateOperator[] operators;

    public AggregateProcessor(String id, AggregateOperator... operators) {
        super(id);
        this.operators = operators;
    }

    @Override
    public void start() {

    }

    @Override
    public void process(Event event) {
        for (AggregateOperator operator : this.operators) {
            event.set(operator.getNewName(), operator.apply(event));
            this.getNextProcessor().process(event);
        }
    }

    @Override
    public void process(List<Event> events) {
        int lastIndex = events.size() - 1;
        if (lastIndex >= 0) {
            for (int i = 0; i < lastIndex; i++) {
                for (AggregateOperator operator : this.operators) {
                    operator.apply(events.get(i));
                }
            }
            // Set the attribute only in last event
            Event lastEvent = events.get(lastIndex);

            for (AggregateOperator operator : this.operators) {
                lastEvent.set(operator.getNewName(), operator.apply(lastEvent));
                // Reset the operator
                operator.clear();
            }
            this.getNextProcessor().process(lastEvent);
        }
    }

    @Override
    public Processor copy() {

        AggregateProcessor mapProcessor = new AggregateProcessor(this.id, this.operators);
        mapProcessor.setNextProcessor(this.getNextProcessor().copy());
        return mapProcessor;
    }
}
