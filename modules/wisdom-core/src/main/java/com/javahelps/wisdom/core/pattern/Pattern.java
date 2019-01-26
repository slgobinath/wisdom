/*
 * Copyright (c) 2018, Gobinath Loganathan (http://github.com/slgobinath) All Rights Reserved.
 *
 * Gobinath licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. In addition, if you are using
 * this file in your research work, you are required to cite
 * WISDOM as mentioned at https://github.com/slgobinath/wisdom.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.javahelps.wisdom.core.pattern;

import com.javahelps.wisdom.core.WisdomApp;
import com.javahelps.wisdom.core.event.Attribute;
import com.javahelps.wisdom.core.event.Event;
import com.javahelps.wisdom.core.exception.WisdomAppValidationException;
import com.javahelps.wisdom.core.processor.Stateful;
import com.javahelps.wisdom.core.processor.StreamProcessor;
import com.javahelps.wisdom.core.stream.Stream;
import com.javahelps.wisdom.core.time.TimestampGenerator;
import com.javahelps.wisdom.core.util.Action;
import com.javahelps.wisdom.core.util.FunctionalUtility;
import com.javahelps.wisdom.core.util.WisdomConstants;

import java.util.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.function.Supplier;

/**
 * The most basic component of a Wisdom definePattern. A definePattern must have a name and optionally a filter.
 */
public class Pattern extends StreamProcessor implements Stateful {

    /**
     * User defined unique name to refer this definePattern.
     */
    protected String name;
    protected List<String> streamIds = new ArrayList<>();
    protected Predicate<Event> filter = FunctionalUtility.truePredicate();
    protected EventDistributor eventDistributor = new EventDistributor();
    protected Lock lock = new ReentrantLock();
    protected AttributeCache attributeCache = new AttributeCache();
    private boolean consumed = false;
    private boolean accepting = true;
    private TimestampGenerator timestampGenerator;
    private boolean batchPattern = false;
    private List<Event> events = new ArrayList<>();
    private Map<Event, Event> eventMap = new HashMap<>();
    private Consumer<Event> postProcess = FunctionalUtility.silentConsumer();
    private Consumer<Event> preProcess = FunctionalUtility.silentConsumer();

    private Predicate<Event> emitConditionMet = event -> consumed;
    private Predicate<Event> processConditionMet = event -> accepting;
    private BiFunction<Event, Event, Boolean> expiredCondition = (currentEvent, oldEvent) -> false;
    private Supplier<List<Event>> previousEvents = () -> {
        ArrayList<Event> arrayList = new ArrayList<>();
        arrayList.add(new Event(timestampGenerator.currentTimestamp()));
        return arrayList;
    };
    private CopyEventAttributes copyEventAttributes = (pattern, src, destination) -> {
        for (Map.Entry<String, Object> entry : src.getData().entrySet()) {
            String key = this.name + "." + entry.getKey();
            Object value = entry.getValue();
            destination.set(key, value);
            this.attributeCache.set(key, value);
        }
    };
    protected Action globalResetAction = this::reset;


    public Pattern(String patternId) {
        super(patternId);
    }

    public Pattern(String patternId, String streamId, String alias) {
        this(patternId);
        this.name = alias;
        this.streamIds.add(streamId);
    }

    public static TimeConstrainedPattern followedBy(Pattern first, Pattern following) {

        return new FollowingPattern(first.id + WisdomConstants.PATTERN_FOLLOWED_BY_INFIX + following.id, first,
                following);
    }

    public static Pattern followedBy(Pattern first, Pattern following, long withinTimestamp) {

        FollowingPattern followingPattern = new FollowingPattern(first.id + WisdomConstants.PATTERN_FOLLOWED_BY_INFIX + following.id, first,
                following);
        followingPattern.setWithin(withinTimestamp);
        return followingPattern;
    }

    public static Pattern and(Pattern first, Pattern second) {

        return new LogicalPattern(first.id + WisdomConstants.PATTERN_AND_INFIX + second.id,
                LogicalPattern.Type.AND, first, second);
    }

    public static Pattern or(Pattern first, Pattern second) {

        return new LogicalPattern(first.id + WisdomConstants.PATTERN_OR_INFIX + second.id,
                LogicalPattern.Type.OR, first, second);
    }

    public static TimeConstrainedPattern not(Pattern pattern) {

        return new NotPattern(WisdomConstants.PATTERN_NOT_PREFIX + pattern.id, pattern);
    }

    public static Pattern every(Pattern pattern) {

        return new EveryPattern(WisdomConstants.PATTERN_EVERY_PREFIX + pattern.id, pattern);
    }

    public void init(WisdomApp wisdomApp) {

        this.timestampGenerator = wisdomApp.getContext().getTimestampGenerator();
        for (String streamId : this.streamIds) {
            Stream stream = wisdomApp.getStream(streamId);
            if (stream == null) {
                throw new WisdomAppValidationException("Stream %s not found", streamId);
            }
            stream.addProcessor(this);
        }
    }

    public void setPostProcess(Consumer<Event> postProcess) {
        this.postProcess = postProcess;
    }

    public void setPreProcess(Consumer<Event> preProcess) {
        this.preProcess = preProcess;
    }

    public boolean isBatchPattern() {
        return batchPattern;
    }

    public void setBatchPattern(boolean batchPattern) {
        this.batchPattern = batchPattern;
    }

    public boolean isComplete() {
        return !this.events.isEmpty();
    }

    public Map<Event, Event> getEventMap() {
        return eventMap;
    }

    public boolean isAccepting() {
        return accepting;
    }

    public void setAccepting(boolean accepting) {
        this.accepting = accepting;
    }

    public void setEmitConditionMet(Predicate<Event> emitConditionMet) {
        this.emitConditionMet = emitConditionMet;
    }

    public Predicate<Event> getProcessConditionMet() {
        return processConditionMet;
    }

    public void setProcessConditionMet(Predicate<Event> processConditionMet) {
        this.processConditionMet = processConditionMet;
    }

    public void setCopyEventAttributes(CopyEventAttributes copyEventAttributes) {
        this.copyEventAttributes = copyEventAttributes;
    }

    public Pattern filter(Predicate<Event> predicate) {
        this.filter = predicate;
        return this;
    }

    public List<Event> getEvents() {
        return events;
    }

    public void setEvents(List<Event> events) {
        this.events = events;
    }

    public Pattern times(long minCount, long maxCount) {

        CountPattern countPattern = new CountPattern(this.id, this, minCount, maxCount);
        return countPattern;
    }

    public Pattern times(long count) {

        return this.times(count, count);
    }

    public Pattern maxTimes(long maxCount) {

        return this.times(0, maxCount);
    }

    public Pattern minTimes(long minCount) {

        return this.times(minCount, Long.MAX_VALUE);
    }

    public boolean isConsumed() {
        return consumed;
    }

    public void setConsumed(boolean consumed) {
        this.consumed = consumed;
    }

    public Supplier<List<Event>> getPreviousEvents() {
        return previousEvents;
    }

    public void setPreviousEvents(Supplier<List<Event>> previousEvents) {
        this.previousEvents = previousEvents;
    }

    public Event event() {
        return this.event(0);
    }

    public Attribute attribute(String attribute) {
        return new Attribute(this.attributeCache, attribute);
    }

    public AttributeCache getAttributeCache() {
        return attributeCache;
    }

    public void setAttributeCache(AttributeCache attributeCache) {
        this.attributeCache = attributeCache;
    }

    public Event last() {
        return this.events.get(this.events.size() - 1);
    }

    public void setExpiredCondition(BiFunction<Event, Event, Boolean> expiredCondition) {
        this.expiredCondition = expiredCondition;
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
        this.accepting = true;
        this.consumed = false;
    }

    @Override
    public void start() {

    }

    @Override
    public void process(Event event) {

        try {
            this.lock.lock();

            consumed = false;
            if (this.processConditionMet.test(event) && this.filter.test(event)) {

                this.preProcess.accept(event);

                Iterator<Event> events = this.previousEvents.get().iterator();
                Event newEvent = null;
                while (events.hasNext()) {

                    Event preEvent = events.next();

                    // Remove the expired events
                    if (this.expiredCondition.apply(event, preEvent)) {
                        events.remove();
                        continue;
                    }

                    newEvent = new Event(event.getStream(), event.getTimestamp());
                    newEvent.setOriginal(event);
                    newEvent.setName(this.name);
                    this.copyEventAttributes.copy(this, event, newEvent);

                    newEvent.getData().putAll(preEvent.getData());

                    this.events.add(newEvent);
                    this.eventMap.put(event.getOriginal(), newEvent);
                }

                if (newEvent != null) {

                    this.accepting = false;
                    this.consumed = true;
                    this.postProcess.accept(newEvent);

                    if (this.emitConditionMet.test(newEvent)) {
                        if (batchPattern) {
                            List<Event> eventsToEmit = new ArrayList<>();
                            eventsToEmit.addAll(this.events);
                            this.globalResetAction.execute();
                            this.getNextProcessor().process(eventsToEmit);
                        } else {
                            for (Event e : this.events) {
                                if (e != newEvent) {
                                    newEvent.getData().putAll(e.getData());
                                }
                            }
                            this.globalResetAction.execute();
                            this.getNextProcessor().process(newEvent);
                        }
                    }
                }
            }
        } finally {
            this.lock.unlock();
        }
    }

    @Override
    public void process(List<Event> events) {

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

    @Override
    public Pattern copy() {
        return null;
    }

    @Override
    public void destroy() {

    }

    @Override
    public void clear() {
        try {
            this.lock.lock();
            this.events.clear();
            this.eventMap.clear();
        } finally {
            this.lock.unlock();
        }
    }

    @FunctionalInterface
    protected interface CopyEventAttributes {
        void copy(Pattern pattern, Event src, Event destination);
    }
}
