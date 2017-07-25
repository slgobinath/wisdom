package com.javahelps.wisdom.core.pattern;

import com.javahelps.wisdom.core.WisdomApp;
import com.javahelps.wisdom.core.event.Event;
import com.javahelps.wisdom.core.processor.StreamProcessor;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Predicate;

/**
 * The most basic component of a Wisdom pattern. A pattern must have a name and optionally a filter.
 */
public class Pattern extends StreamProcessor {

    protected String name;
    protected List<String> streamIds = new ArrayList<>();
    protected Predicate<Event> predicate = event -> true;
    protected Duration duration;
    private Predicate<Event> emitConditionMet = event -> false;
    private Predicate<Event> processConditionMet = event -> false;
    private boolean waiting = true;
    private List<Event> events = new ArrayList<>();
    protected EventDistributor eventDistributor = new EventDistributor();
    private Consumer<Event> mergePreviousEvents = event -> {
    };
    private CopyEventAttributes copyEventAttributes = (pattern, src, destination) -> {
        for (Map.Entry<String, Comparable> entry : src.getData().entrySet()) {
            destination.set(this.name + "." + entry.getKey(), entry.getValue());
        }
    };

    private Consumer<Event> afterProcess = event -> {
    };


    public Pattern(String patternId) {
        super(patternId);
    }

    public Pattern(String patternId, String name, String streamId) {
        this(patternId);
        this.name = name;
        this.streamIds.add(streamId);
    }

    public void init(WisdomApp wisdomApp) {

        this.streamIds.forEach(streamId -> wisdomApp.getStream(streamId).addProcessor(this));
    }

    public static Pattern pattern(String patternId, String id, String streamId) {
        Pattern pattern = new Pattern(patternId, id, streamId);
        return pattern;
    }

    public void setAfterProcess(Consumer<Event> afterProcess) {
        this.afterProcess = afterProcess;
    }

    public void setEmitConditionMet(Predicate<Event> emitConditionMet) {
        this.emitConditionMet = emitConditionMet;
    }

    public void setProcessConditionMet(Predicate<Event> processConditionMet) {
        this.processConditionMet = processConditionMet;
    }

    public Predicate<Event> getProcessConditionMet() {
        return processConditionMet;
    }

    public void setCopyEventAttributes(CopyEventAttributes copyEventAttributes) {
        this.copyEventAttributes = copyEventAttributes;
    }

    public Pattern filter(Predicate<Event> predicate) {
        this.predicate = predicate;
        return this;
    }

    public List<Event> getEvents() {
        return events;
    }

    public Pattern times(int minCount, int maxCount) {

        CountPattern countPattern = new CountPattern(this.id, this, minCount, maxCount);
        return countPattern;
    }

    public static Pattern followedBy(String id, Pattern first, Pattern following) {

        return new FollowingPattern(id, first, following);
    }

    public static Pattern and(String id, Pattern first, Pattern second) {

        LogicalPattern logicalPattern = new LogicalPattern(id, LogicalPattern.Type.AND, first, second);
        return logicalPattern;
    }

    public static Pattern or(String id, Pattern first, Pattern second) {

        LogicalPattern logicalPattern = new LogicalPattern(id, LogicalPattern.Type.OR, first, second);
        return logicalPattern;
    }

    public static Pattern not(String id, Pattern pattern) {

        NotPattern notPattern = new NotPattern(id, pattern);
        return notPattern;
    }

    public boolean isWaiting() {
        return waiting;
    }

    public Consumer<Event> getMergePreviousEvents() {
        return mergePreviousEvents;
    }

    public void setMergePreviousEvents(Consumer<Event> mergePreviousEvents) {
        this.mergePreviousEvents = mergePreviousEvents;
    }

    public void setEvents(List<Event> events) {
        this.events = events;
    }

    public Event event() {
        return this.event(0);
    }

    public Event event(int index) {
        Event event = null;
        if (index < this.events.size()) {
            event = this.events.get(index);
        }
        return event;
    }

    public void reset() {
        this.events.clear();
        this.waiting = true;
    }

    @Override
    public void start() {

    }

    @Override
    public void process(Event event) {
        if (this.processConditionMet.test(event) && this.predicate.test(event)) {

            Event newEvent = new Event(event.getStream(), event.getTimestamp());
            newEvent.setName(this.name);
            this.copyEventAttributes.copy(this, event, newEvent);
            this.mergePreviousEvents.accept(newEvent);
            this.events.add(newEvent);
            this.waiting = false;

            this.afterProcess.accept(newEvent);

            if (this.emitConditionMet.test(newEvent)) {
                for (Event e : this.events) {
                    if (e != newEvent) {
                        newEvent.getData().putAll(e.getData());
                    }
                }
                this.reset();
                this.getNextProcessor().process(newEvent);
            }
        }
    }

    @Override
    public void process(Collection<Event> events) {

    }

    public void previousEventProcessed(Event event) {

    }

    public Pattern within(Duration duration) {

        this.duration = duration;
        return this;
    }

    @FunctionalInterface
    protected interface CopyEventAttributes {
        void copy(Pattern pattern, Event src, Event destination);
    }
}
