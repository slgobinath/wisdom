package com.javahelps.wisdom.core.query;

import com.javahelps.wisdom.core.WisdomApp;
import com.javahelps.wisdom.core.event.Event;
import com.javahelps.wisdom.core.exception.WisdomAppValidationException;
import com.javahelps.wisdom.core.pattern.Pattern;
import com.javahelps.wisdom.core.processor.FilterProcessor;
import com.javahelps.wisdom.core.processor.MapProcessor;
import com.javahelps.wisdom.core.processor.PatternProcessor;
import com.javahelps.wisdom.core.processor.SelectProcessor;
import com.javahelps.wisdom.core.processor.StreamProcessor;
import com.javahelps.wisdom.core.processor.WindowProcessor;
import com.javahelps.wisdom.core.stream.Stream;
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
    private int processorIndex;

    public Query(WisdomApp wisdomApp, String id) {

        this.wisdomApp = wisdomApp;
        this.id = id;
    }

    public Query from(String streamId) {

        this.inputStream = this.wisdomApp.getStream(streamId);
        return this;
    }

    public Query from(Pattern pattern) {

        // Find the last pattern
        Pattern last = pattern;
        if (last.getNextPattern() != null) {
            last = last.getNextPattern();
        }
        last.setLast(true);

        // Find the first pattern
        Pattern first = pattern;
        while (!first.isFirst()) {
            first = first.getPreviousPattern();
        }

        PatternProcessor patternProcessor = new PatternProcessor(generateId(), this.wisdomApp, first);
        if (this.lastStreamProcessor != null) {
            throw new WisdomAppValidationException("Pattern must be the first operator in the query");
        }

        Pattern tempPattern = first;
        while (tempPattern != null) {
            this.wisdomApp.getStream(tempPattern.getStreamId()).addProcessor(patternProcessor);
            tempPattern = tempPattern.getNextPattern();
        }
        this.lastStreamProcessor = patternProcessor;
        return this;
    }

    public Query filter(Predicate<Event> predicate) {

        FilterProcessor filterProcessor = new FilterProcessor(generateId(), this.inputStream, predicate);
        if (this.lastStreamProcessor == null) {
            this.inputStream.addProcessor(filterProcessor);
        } else {
            this.lastStreamProcessor.setNextProcessor(filterProcessor);
        }
        this.lastStreamProcessor = filterProcessor;

        return this;
    }

    public Query window(Window window) {

        WindowProcessor windowProcessor = new WindowProcessor(generateId(), this.inputStream, window);
        if (this.lastStreamProcessor == null) {
            this.inputStream.addProcessor(windowProcessor);
        } else {
            this.lastStreamProcessor.setNextProcessor(windowProcessor);
        }
        this.lastStreamProcessor = windowProcessor;
        return this;
    }

    public Query select(String... attributes) {

        SelectProcessor selectProcessor = new SelectProcessor(generateId(), this.inputStream, attributes);
        if (this.lastStreamProcessor == null) {
            this.inputStream.addProcessor(selectProcessor);
        } else {
            this.lastStreamProcessor.setNextProcessor(selectProcessor);
        }
        this.lastStreamProcessor = selectProcessor;

        return this;
    }

    public Query map(Function<Event, Event> function) {

        MapProcessor mapProcessor = new MapProcessor(generateId(), this.inputStream, function);
        if (this.lastStreamProcessor == null) {
            this.inputStream.addProcessor(mapProcessor);
        } else {
            this.lastStreamProcessor.setNextProcessor(mapProcessor);
        }
        this.lastStreamProcessor = mapProcessor;

        return this;
    }

    public Query having(Predicate<Event> predicate) {

        FilterProcessor filterProcessor = new FilterProcessor(generateId(), this.inputStream, predicate);
        if (this.lastStreamProcessor == null) {
            this.inputStream.addProcessor(filterProcessor);
        } else {
            this.lastStreamProcessor.setNextProcessor(filterProcessor);
        }
        this.lastStreamProcessor = filterProcessor;

        return this;
    }

    public Query insertInto(String streamId) {

        this.outputStream = this.wisdomApp.getStream(streamId);

        if (this.lastStreamProcessor == null) {
            // No processors in between
            this.inputStream.addProcessor(this.outputStream);
        } else {
            this.lastStreamProcessor.setNextProcessor(this.outputStream);
        }

        return this;
    }

    private String generateId() {
        return String.format("%s[%d]", this.id, this.processorIndex);
    }
}
