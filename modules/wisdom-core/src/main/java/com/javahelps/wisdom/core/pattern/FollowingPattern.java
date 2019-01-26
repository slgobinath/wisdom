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

import com.javahelps.wisdom.core.event.Event;
import com.javahelps.wisdom.core.processor.Processor;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.function.Supplier;

/**
 * Created by gobinath on 6/29/17.
 */
class FollowingPattern extends TimeConstrainedPattern {


    private Pattern first;
    private Pattern next;

    FollowingPattern(String patternId, Pattern first, Pattern next) {
        super(patternId, first, next);
        this.first = first;
        this.next = next;
        this.eventDistributor.add(first);
        this.eventDistributor.add(next);

        this.first.setProcessConditionMet(event -> true);
        this.next.setProcessConditionMet(event -> this.first.isComplete());

        this.first.setEmitConditionMet(event -> false);
        this.next.setEmitConditionMet(event -> true);

        this.next.setPreviousEvents(this.first::getEvents);

        this.first.setPreProcess(event -> this.next.onPreviousPreProcess(event));
        this.first.setPostProcess(event -> this.next.onPreviousPostProcess(event));

        this.next.setPreProcess(event -> this.first.onNextPreProcess(event));
        this.next.setPostProcess(event -> this.first.onNextPostProcess(event));

        if (this.first.isBatchPattern()) {
            this.next.setBatchPattern(true);
        }
        if (this.next.isBatchPattern()) {
            this.setBatchPattern(true);
        }

        // Calling event from any of these patterns should be from the last one
        this.attributeCache.setMap(first.attributeCache.getMap());
        this.next.attributeCache.setMap(first.attributeCache.getMap());
    }

    @Override
    public void onNextPreProcess(Event event) {
        this.next.onNextPreProcess(event);
    }

    @Override
    public void onPreviousPreProcess(Event event) {
        this.first.onPreviousPreProcess(event);
    }

    @Override
    public void onPreviousPostProcess(Event event) {
        this.first.onPreviousPostProcess(event);
    }

    @Override
    public Event event() {
        return next.event();
    }

    @Override
    public boolean isAccepting() {
        return first.isAccepting() || next.isAccepting();
    }

    @Override
    public void setAccepting(boolean accepting) {
        if (accepting) {
            next.setAccepting(true);
        } else {
            first.setAccepting(false);
            next.setAccepting(false);
        }
    }

    @Override
    public void setProcessConditionMet(Predicate<Event> processConditionMet) {
        this.first.setProcessConditionMet(processConditionMet);
    }

    @Override
    public void setEmitConditionMet(Predicate<Event> emitConditionMet) {
        this.next.setEmitConditionMet(emitConditionMet);
    }

    @Override
    public void setNextProcessor(Processor nextProcessor) {
        super.setNextProcessor(nextProcessor);
        this.first.setNextProcessor(nextProcessor);
        this.next.setNextProcessor(nextProcessor);
    }

    @Override
    public void process(Event event) {
        try {
            this.lock.lock();
            this.first.setConsumed(false);
            this.next.setConsumed(false);
            this.eventDistributor.process(event);
        } finally {
            this.lock.unlock();
        }
    }

    @Override
    public boolean isConsumed() {
        return this.first.isConsumed() || this.next.isConsumed();
    }

    @Override
    public void setConsumed(boolean consumed) {
        this.first.setConsumed(false);
        this.next.setConsumed(false);
    }

    @Override
    public boolean isComplete() {
        return this.next.isComplete();
    }

    @Override
    public void setPreviousEvents(Supplier<List<Event>> previousEvents) {
        this.first.setPreviousEvents(previousEvents);
    }

    @Override
    public void setBatchPattern(boolean batchPattern) {
        super.setBatchPattern(batchPattern);
        this.first.setBatchPattern(batchPattern);
        this.next.setBatchPattern(batchPattern);
    }

    @Override
    public List<Event> getEvents() {

        List<Event> events = new ArrayList<>();
        if (this.next instanceof EmptiablePattern) {
            events.addAll(((EmptiablePattern) this.next).getEvents(false));
        } else {
            events.addAll(this.next.getEvents());
        }

        return events;
    }

    @Override
    public void setPostProcess(Consumer<Event> postProcess) {
        this.next.setPostProcess(postProcess);
    }

    @Override
    public Pattern within(Duration duration) {
        long period = duration.toMillis();
        this.next.setExpiredCondition((currentEvent, preEvent) -> currentEvent.getTimestamp() - preEvent.getTimestamp
                () > period);
        return this;
    }

    public void setWithin(long timestamp) {

    }

    @Override
    public void setExpiredCondition(BiFunction<Event, Event, Boolean> expiredCondition) {
        this.next.setExpiredCondition(expiredCondition);
    }

    @Override
    public void clear() {
        try {
            this.lock.lock();
            this.first.clear();
            this.next.clear();
        } finally {
            this.lock.unlock();
        }
    }
}
