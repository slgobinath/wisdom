package com.javahelps.wisdom.core.pattern;

import com.javahelps.wisdom.core.WisdomApp;
import com.javahelps.wisdom.core.event.Event;
import com.javahelps.wisdom.core.processor.StreamProcessor;
import com.javahelps.wisdom.core.util.WisdomConstants;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
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
    private boolean waiting = true;
    private Predicate<Event> emitConditionMet = event -> !waiting;
    private Predicate<Event> processConditionMet = event -> waiting;
    private List<Event> events = new ArrayList<>();
    protected EventDistributor eventDistributor = new EventDistributor();
    private Consumer<Event> mergePreviousEvents = event -> {
    };
    private CopyEventAttributes copyEventAttributes = (pattern, src, destination) -> {
        for (Map.Entry<String, Comparable> entry : src.getData().entrySet()) {
            destination.set(this.name + "." + entry.getKey(), entry.getValue());
        }
    };

    private Consumer<Event> postProcess = event -> {
    };
    private Consumer<Event> preProcess = event -> {
    };
    private Map<Event, Event> eventMap = new HashMap<>();


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

    public static Pattern pattern(String patternId, String name, String streamId) {
        Pattern pattern = new Pattern(patternId, name, streamId);
        return pattern;
    }

    public void setPostProcess(Consumer<Event> postProcess) {
        this.postProcess = postProcess;
    }

    public void setPreProcess(Consumer<Event> preProcess) {
        this.preProcess = preProcess;
    }

    public Map<Event, Event> getEventMap() {
        return eventMap;
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

    public Pattern times(int count) {

        return this.times(count, count);
    }

    public Pattern maxTimes(int maxCount) {

        return this.times(0, maxCount);
    }

    public Pattern minTimes(int minCount) {

        return this.times(minCount, Integer.MAX_VALUE);
    }

    public static Pattern followedBy(Pattern first, Pattern following) {

        return new FollowingPattern(first.id + WisdomConstants.PATTERN_FOLLOWED_BY_INFIX + following.id, first, following);
    }

    public static Pattern and(Pattern first, Pattern second) {

        LogicalPattern logicalPattern = new LogicalPattern(first.id + WisdomConstants.PATTERN_AND_INFIX + second.id,
                LogicalPattern.Type.AND, first, second);
        return logicalPattern;
    }

    public static Pattern or(Pattern first, Pattern second) {

        LogicalPattern logicalPattern = new LogicalPattern(first.id + WisdomConstants.PATTERN_OR_INFIX + second.id,
                LogicalPattern.Type.OR, first, second);
        return logicalPattern;
    }

    public static Pattern not(Pattern pattern) {

        NotPattern notPattern = new NotPattern(WisdomConstants.PATTERN_NOT_PREFIX + pattern.id, pattern);
        return notPattern;
    }

    public static Pattern every(Pattern pattern) {

        EveryPattern everyPattern = new EveryPattern(WisdomConstants.PATTERN_EVERY_PREFIX + pattern.id, pattern);
        return everyPattern;
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

    public Event last() {
        return this.events.get(this.events.size() - 1);
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
        this.eventMap.clear();
        this.waiting = true;
    }

    @Override
    public void start() {

    }

    @Override
    public void process(Event event) {

        if (this.processConditionMet.test(event) && this.predicate.test(event)) {

            this.preProcess.accept(event);

            Event newEvent = new Event(event.getStream(), event.getTimestamp());
            newEvent.setOriginal(event);
            newEvent.setName(this.name);
            this.copyEventAttributes.copy(this, event, newEvent);
            this.mergePreviousEvents.accept(newEvent);
            this.events.add(newEvent);
            this.eventMap.put(event.getOriginal(), newEvent);
            this.waiting = false;

            this.postProcess.accept(newEvent);

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

    public void onPreviousPostProcess(Event event) {

    }

    public void onPreviousPreProcess(Event event) {

    }

    public void onNextPostProcess(Event event) {
        this.reset();
    }

    public void onNextPreProcess(Event event) {

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
