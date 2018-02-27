package com.javahelps.wisdom.core.query;

import com.javahelps.wisdom.core.WisdomApp;
import com.javahelps.wisdom.core.event.Event;
import com.javahelps.wisdom.core.event.Index;
import com.javahelps.wisdom.core.exception.WisdomAppValidationException;
import com.javahelps.wisdom.core.pattern.Pattern;
import com.javahelps.wisdom.core.processor.AggregateProcessor;
import com.javahelps.wisdom.core.processor.AttributeSelectProcessor;
import com.javahelps.wisdom.core.processor.EventSelectProcessor;
import com.javahelps.wisdom.core.processor.FilterProcessor;
import com.javahelps.wisdom.core.processor.MapProcessor;
import com.javahelps.wisdom.core.processor.PartitionProcessor;
import com.javahelps.wisdom.core.processor.StreamProcessor;
import com.javahelps.wisdom.core.processor.WindowProcessor;
import com.javahelps.wisdom.core.stream.Stream;
import com.javahelps.wisdom.core.variable.Variable;
import com.javahelps.wisdom.core.window.Window;

import java.util.function.Function;
import java.util.function.Predicate;

/**
 * {@link Query} is the complete executable component with the self contained logic to process the events from an
 * input {@link Stream} and insert the outputs into an output {@link Stream}.
 */
public class Query {

    private String id;
    private WisdomApp wisdomApp;
    private Stream inputStream;
    private Stream outputStream;
    private StreamProcessor lastStreamProcessor;
    private int processorIndex = 0;

    public Query(WisdomApp wisdomApp, String id) {

        this.wisdomApp = wisdomApp;
        this.id = id;
    }

    public Query from(String streamId) {

        this.inputStream = this.wisdomApp.getStream(streamId);
        if (this.inputStream == null) {
            throw new WisdomAppValidationException("Stream %s is not defined in %s", streamId, this.wisdomApp.getName());
        }
        return this;
    }

    public Query from(Pattern pattern) {

        this.lastStreamProcessor = pattern;
        this.wisdomApp.addStreamProcessor(pattern);
        return this;
    }

    public Query filter(Predicate<Event> predicate) {

        FilterProcessor filterProcessor = new FilterProcessor(generateId(), predicate);
        if (this.lastStreamProcessor == null) {
            this.inputStream.addProcessor(filterProcessor);
        } else {
            this.lastStreamProcessor.setNextProcessor(filterProcessor);
        }
        this.lastStreamProcessor = filterProcessor;

        return this;
    }

    public Query window(Window window) {

        WindowProcessor windowProcessor = new WindowProcessor(generateId(), window);
        if (this.lastStreamProcessor == null) {
            this.inputStream.addProcessor(windowProcessor);
        } else {
            this.lastStreamProcessor.setNextProcessor(windowProcessor);
        }
        this.lastStreamProcessor = windowProcessor;
        return this;
    }

    public Query select(String... attributes) {

        AttributeSelectProcessor attributeSelectProcessor = new AttributeSelectProcessor(generateId(), attributes);
        if (this.lastStreamProcessor == null) {
            this.inputStream.addProcessor(attributeSelectProcessor);
        } else {
            this.lastStreamProcessor.setNextProcessor(attributeSelectProcessor);
        }
        this.lastStreamProcessor = attributeSelectProcessor;

        attributeSelectProcessor.init();

        return this;
    }

    public Query select(Index index) {

        EventSelectProcessor eventSelectProcessor = new EventSelectProcessor(generateId(), index);
        if (this.lastStreamProcessor == null) {
            this.inputStream.addProcessor(eventSelectProcessor);
        } else {
            this.lastStreamProcessor.setNextProcessor(eventSelectProcessor);
        }
        this.lastStreamProcessor = eventSelectProcessor;

        eventSelectProcessor.init();

        return this;
    }

    public Query map(Function<Event, Event> function) {

        MapProcessor mapProcessor = new MapProcessor(generateId(), function);
        if (this.lastStreamProcessor == null) {
            this.inputStream.addProcessor(mapProcessor);
        } else {
            this.lastStreamProcessor.setNextProcessor(mapProcessor);
        }
        this.lastStreamProcessor = mapProcessor;

        return this;
    }

    public Query aggregate(Function<Event, Comparable> function, String setAs) {

        AggregateProcessor aggregateProcessor = new AggregateProcessor(generateId(), function, setAs);
        if (this.lastStreamProcessor == null) {
            this.inputStream.addProcessor(aggregateProcessor);
        } else {
            this.lastStreamProcessor.setNextProcessor(aggregateProcessor);
        }
        this.lastStreamProcessor = aggregateProcessor;

        return this;
    }

    public Query having(Predicate<Event> predicate) {

        FilterProcessor filterProcessor = new FilterProcessor(generateId(), predicate);
        if (this.lastStreamProcessor == null) {
            this.inputStream.addProcessor(filterProcessor);
        } else {
            this.lastStreamProcessor.setNextProcessor(filterProcessor);
        }
        this.lastStreamProcessor = filterProcessor;

        return this;
    }

    public void insertInto(String streamId) {

        this.outputStream = this.wisdomApp.getStream(streamId);

        if (this.outputStream == null) {
            throw new WisdomAppValidationException("Stream %s is not defined in %s", streamId, this.wisdomApp.getName());
        }
        if (this.lastStreamProcessor == null) {
            // No processors in between
            this.inputStream.addProcessor(this.outputStream);
        } else {
            this.lastStreamProcessor.setNextProcessor(this.outputStream);
        }
    }

    public void update(String variableId) {

        Variable variable = this.wisdomApp.getVariable(variableId);

        if (this.lastStreamProcessor == null) {
            // No processors in between
            this.inputStream.addProcessor(variable);
        } else {
            this.lastStreamProcessor.setNextProcessor(variable);
        }
    }

    public Query partitionBy(String... attributes) {

        PartitionProcessor partitionProcessor = new PartitionProcessor(generateId(), attributes);
        if (this.lastStreamProcessor == null) {
            this.inputStream.addProcessor(partitionProcessor);
        } else {
            this.lastStreamProcessor.setNextProcessor(partitionProcessor);
        }
        this.lastStreamProcessor = partitionProcessor;

        return this;
    }

    private String generateId() {
        return String.format("%s[%d]", this.id, this.processorIndex++);
    }
}
