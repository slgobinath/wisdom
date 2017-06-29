package com.javahelps.wisdom.core.pattern;

import com.javahelps.wisdom.core.WisdomApp;
import com.javahelps.wisdom.core.event.Event;
import com.javahelps.wisdom.core.processor.Processor;
import com.javahelps.wisdom.core.stream.Stream;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;

/**
 * Created by gobinath on 6/29/17.
 */
public class Pattern extends Stream {

    private String name;
    private String streamId;
    private Predicate<Event> predicate = event -> true;
    private int minCount;
    private int maxCount;
    private Pattern previousPattern;
    private Pattern nextPattern;
    private boolean first;
    private boolean last;
    private boolean waiting = true;
    private List<Event> events = new ArrayList<>();
    private Processor nextProcessor;
    private List<String> attributesList = new ArrayList<>();


    public Pattern(String patternId) {
        super(patternId);
    }

    public Pattern(String patternId, String name, String streamId) {
        this(patternId);
        this.name = name;
        this.streamId = streamId;
    }

    public void init(WisdomApp wisdomApp, Processor nextProcessor) {

        this.nextProcessor = nextProcessor;

        // Add the attributesList of previous patterns
        Pattern pattern = this.previousPattern;
        while (pattern != null) {
            this.attributesList.addAll(pattern.attributesList);
            pattern = pattern.previousPattern;
        }

        // Add the attributesList of the input stream
        String[] streamAttributes = wisdomApp.getStream(this.streamId).getAttributes();
        int noOfAttributes = streamAttributes.length;
        for (int i = 0; i < noOfAttributes; i++) {
            this.attributesList.add(this.name + "." + streamAttributes[i]);
        }
        this.attributes = this.attributesList.toArray(new String[0]);
    }

    public static Pattern begin(String patternId, String id, String streamId) {
        Pattern pattern = new Pattern(patternId, id, streamId);
        pattern.first = true;
        return pattern;
    }

    public Pattern filter(Predicate<Event> predicate) {
        this.predicate = predicate;
        return this;
    }

    public Pattern times(int minCount, int maxCount) {
        this.minCount = minCount;
        this.maxCount = maxCount;
        return this;
    }

    public Pattern next(String id, String streamId) {
        Pattern next = new Pattern(getStreamId(), id, streamId);
        this.nextPattern = next;
        next.previousPattern = this;
        return next;
    }

    public Pattern followedBy(String id, String streamId) {
        Pattern next = new Pattern(getStreamId(), id, streamId);
        this.nextPattern = next;
        next.previousPattern = this;
        return next;
    }

    public Pattern and(String id, String streamId) {
        Pattern next = new Pattern(getStreamId(), id, streamId);
        this.nextPattern = next;
        next.previousPattern = this;
        return next;
    }

    public Pattern or(String id, String streamId) {
        Pattern next = new Pattern(getStreamId(), id, streamId);
        this.nextPattern = next;
        next.previousPattern = this;
        return next;
    }

    public Pattern not(String id, String streamId) {
        Pattern next = new Pattern(getStreamId(), id, streamId);
        this.nextPattern = next;
        next.previousPattern = this;
        return next;
    }

    public boolean isFirst() {
        return first;
    }

    public boolean isLast() {
        return last;
    }

    public void setLast(boolean last) {
        this.last = last;
    }

    public boolean isWaiting() {
        return waiting;
    }

    public Pattern getPreviousPattern() {
        return previousPattern;
    }

    public Pattern getNextPattern() {
        return nextPattern;
    }

    public String getStreamId() {
        return streamId;
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

    public void process(Event event) {
        if ((this.first || !this.previousPattern.waiting) && this.predicate.test(event)) {
            event.setName(this.name);
            this.events.add(event);
            this.waiting = false;

            if (this.last) {
                List<Event> eventsToEmit = new ArrayList<>();
                Pattern pattern = this;
                while (pattern != null) {
                    eventsToEmit.addAll(pattern.events);
                    pattern.reset();
                    pattern = pattern.previousPattern;
                }
                Event newEvent = new Event(this, event.getTimestamp());
                for (Event e : eventsToEmit) {
                    for (Map.Entry<String, Comparable> entry : e.getData().entrySet()) {
                        newEvent.set(e.getName() + "." + entry.getKey(), entry.getValue());
                    }
                }
                this.nextProcessor.process(newEvent);
            }
        }
    }
}
