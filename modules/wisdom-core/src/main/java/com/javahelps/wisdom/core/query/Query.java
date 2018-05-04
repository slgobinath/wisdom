package com.javahelps.wisdom.core.query;

import com.javahelps.wisdom.core.WisdomApp;
import com.javahelps.wisdom.core.event.Event;
import com.javahelps.wisdom.core.event.Index;
import com.javahelps.wisdom.core.exception.WisdomAppValidationException;
import com.javahelps.wisdom.core.map.Mapper;
import com.javahelps.wisdom.core.operator.AggregateOperator;
import com.javahelps.wisdom.core.pattern.Pattern;
import com.javahelps.wisdom.core.processor.*;
import com.javahelps.wisdom.core.stream.Stream;
import com.javahelps.wisdom.core.variable.Variable;
import com.javahelps.wisdom.core.window.Window;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Predicate;

/**
 * {@link Query} is the complete executable component with the self contained logic to process the events from an
 * input {@link Stream} and insert the outputs into an output {@link Stream}.
 */
public class Query implements Stateful {

    private final Map<String, StreamProcessor> streamProcessorMap = new HashMap<>();
    private final List<Stateful> statefulList = new ArrayList<>();
    private final List<Mapper> mapperList = new ArrayList<>();
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

    /**
     * Initialize all stream processors of the query.
     */
    public void init() {
        this.streamProcessorMap.values().forEach(processor -> processor.init(this.wisdomApp));
        this.mapperList.forEach(mapper -> mapper.init(this.wisdomApp));
    }

    public void start() {
        this.streamProcessorMap.values().forEach(StreamProcessor::start);
        this.mapperList.forEach(Mapper::start);
    }

    public void stop() {
        this.streamProcessorMap.values().forEach(StreamProcessor::stop);
        this.mapperList.forEach(Mapper::stop);
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
        this.addStreamProcessor(pattern);
        return this;
    }

    public Query filter(Predicate<Event> predicate) {

        FilterProcessor filterProcessor = new FilterProcessor(generateId(), predicate);
        if (this.lastStreamProcessor == null) {
            this.inputStream.addProcessor(filterProcessor);
        } else {
            this.lastStreamProcessor.setNextProcessor(filterProcessor);
        }
        this.addStreamProcessor(filterProcessor);
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
        this.addStreamProcessor(windowProcessor);
        this.lastStreamProcessor = windowProcessor;
        return this;
    }

    public Query limit(int... bounds) {

        LimitProcessor limitProcessor = new LimitProcessor(generateId(), bounds);
        if (this.lastStreamProcessor == null) {
            this.inputStream.addProcessor(limitProcessor);
        } else {
            this.lastStreamProcessor.setNextProcessor(limitProcessor);
        }
        this.addStreamProcessor(limitProcessor);
        this.lastStreamProcessor = limitProcessor;
        return this;
    }

    public Query select(String... attributes) {

        AttributeSelectProcessor attributeSelectProcessor = new AttributeSelectProcessor(generateId(), attributes);
        if (this.lastStreamProcessor == null) {
            this.inputStream.addProcessor(attributeSelectProcessor);
        } else {
            this.lastStreamProcessor.setNextProcessor(attributeSelectProcessor);
        }
        this.addStreamProcessor(attributeSelectProcessor);
        this.lastStreamProcessor = attributeSelectProcessor;

        return this;
    }

    public Query select(Index index) {

        EventSelectProcessor eventSelectProcessor = new EventSelectProcessor(generateId(), index);
        if (this.lastStreamProcessor == null) {
            this.inputStream.addProcessor(eventSelectProcessor);
        } else {
            this.lastStreamProcessor.setNextProcessor(eventSelectProcessor);
        }
        this.addStreamProcessor(eventSelectProcessor);
        this.lastStreamProcessor = eventSelectProcessor;

        return this;
    }

    public Query map(Function<Event, Event> function) {

        MapProcessor mapProcessor = new MapProcessor(generateId(), function);
        if (this.lastStreamProcessor == null) {
            this.inputStream.addProcessor(mapProcessor);
        } else {
            this.lastStreamProcessor.setNextProcessor(mapProcessor);
        }
        this.addStreamProcessor(mapProcessor);
        this.lastStreamProcessor = mapProcessor;

        return this;
    }

    public Query map(Function<Event, Event>... functions) {
        for (Function<Event, Event> function : functions) {
            this.map(function);
        }
        return this;
    }

    public Query map(Mapper... mappers) {
        for (Mapper mapper : mappers) {
            this.map(mapper);
            this.mapperList.add(mapper);
        }
        return this;
    }

    public Query map(Mapper mapper) {

        MapProcessor mapProcessor = new MapProcessor(generateId(), mapper);
        if (this.lastStreamProcessor == null) {
            this.inputStream.addProcessor(mapProcessor);
        } else {
            this.lastStreamProcessor.setNextProcessor(mapProcessor);
        }
        this.addStreamProcessor(mapProcessor);
        this.lastStreamProcessor = mapProcessor;
        this.mapperList.add(mapper);
        return this;
    }

    public Query aggregate(AggregateOperator... operators) {

        AggregateProcessor aggregateProcessor = new AggregateProcessor(generateId(), operators);
        if (this.lastStreamProcessor == null) {
            this.inputStream.addProcessor(aggregateProcessor);
        } else {
            this.lastStreamProcessor.setNextProcessor(aggregateProcessor);
        }
        this.addStreamProcessor(aggregateProcessor);
        this.lastStreamProcessor = aggregateProcessor;

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
        this.addStreamProcessor(partitionProcessor);

        return this;
    }

    private void addStreamProcessor(StreamProcessor processor) {
        this.streamProcessorMap.put(processor.getId(), processor);
        if (processor instanceof Stateful) {
            this.statefulList.add((Stateful) processor);
        }
    }

    private String generateId() {
        return String.format("%s[%d]", this.id, this.processorIndex++);
    }

    @Override
    public void clear() {
        this.statefulList.forEach(Stateful::clear);
    }
}
