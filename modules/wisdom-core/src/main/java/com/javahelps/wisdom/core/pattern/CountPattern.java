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
import com.javahelps.wisdom.core.event.Event;
import com.javahelps.wisdom.core.processor.Processor;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;
import java.util.function.Supplier;

/**
 * Created by gobinath on 6/29/17.
 */
class CountPattern extends WrappingPattern implements EmptiablePattern {

    private Pattern pattern;
    private long minCount;
    private long maxCount;
    private boolean isFirst = true;

    CountPattern(String patternId, Pattern pattern, long minCount, long maxCount) {

        super(patternId, pattern);

        this.pattern = pattern;
        this.minCount = minCount;
        this.maxCount = maxCount;

        this.pattern.setProcessConditionMet(event -> {
            boolean re = pattern.getEvents().size() < this.maxCount;
            return re;
        });
        this.pattern.setEmitConditionMet(event -> pattern.getEvents().size() >= this.minCount);
        this.pattern.setCopyEventAttributes((pattern1, src, destination) -> {
            for (Map.Entry<String, Object> entry : src.getData().entrySet()) {
                String key = pattern.name + "[" + pattern.getEvents().size() + "]." + entry.getKey();
                Object value = entry.getValue();
                destination.set(key, value);
                this.attributeCache.set(key, value);
            }
        });

        Predicate<Event> predicate = event -> this.pattern.isAccepting();
        this.filter = predicate;

        this.attributeCache.setMap(pattern.attributeCache.getMap());
    }

    @Override
    public void setNextProcessor(Processor nextProcessor) {

        super.setNextProcessor(nextProcessor);
        this.pattern.setNextProcessor(nextProcessor);
    }

    @Override
    public void process(Event event) {

        try {
            this.lock.lock();
            this.pattern.process(event);
        } finally {
            this.lock.unlock();
        }
    }

    @Override
    public void onNextPreProcess(Event event) {
        if (this.minCount == 0) {
            Event eventRemoved = this.pattern.getEventMap().remove(event.getOriginal());
            if (eventRemoved != null) {
                this.pattern.getEvents().remove(eventRemoved);
            }
        }
    }

    @Override
    public List<Event> getEvents() {

        return this.getEvents(isFirst);
    }

    @Override
    public void setProcessConditionMet(Predicate<Event> processConditionMet) {

        processConditionMet = processConditionMet.or(event -> !this.getEvents().isEmpty());
        this.pattern.setProcessConditionMet(this.pattern.getProcessConditionMet().and(processConditionMet));
    }

    @Override
    public void setEmitConditionMet(Predicate<Event> emitConditionMet) {

        Predicate<Event> predicate = this.filter.and(emitConditionMet);
        this.pattern.setEmitConditionMet(predicate);
    }

    @Override
    public boolean isAccepting() {
        return pattern.getEvents().size() < this.maxCount;
    }

    @Override
    public boolean isConsumed() {

        return pattern.isConsumed() && this.minCount > 0 && pattern.getEvents().size() > this.minCount;
    }

    @Override
    public void setConsumed(boolean consumed) {
        this.pattern.setConsumed(false);
    }

    @Override
    public boolean isComplete() {
        return pattern.getEvents().size() >= this.minCount;
    }

//    @Override
//    public void setMergePreviousEvents(Consumer<Event> mergePreviousEvents) {
//
//        super.setMergePreviousEvents(mergePreviousEvents);
//        this.definePattern.setMergePreviousEvents(this.definePattern.getMergePreviousEvents().andThen(mergePreviousEvents));
//    }

    @Override
    public void setPreviousEvents(Supplier<List<Event>> previousEvents) {
        super.setPreviousEvents(previousEvents);
        this.isFirst = false;
        this.pattern.setPreviousEvents(previousEvents);
    }

    @Override
    public List<Event> getEvents(boolean isFirst) {

        List<Event> list = new ArrayList<>();
        List<Event> actualEvents = this.pattern.getEvents();
        if (!actualEvents.isEmpty()) {
            Event event = actualEvents.get(0);
            for (int i = 1; i < actualEvents.size(); i++) {
                event.getData().putAll(actualEvents.get(i).getData());
            }
            list.add(event);
        } else {
            if (isFirst) {
                list.add(EmptiablePattern.EMPTY_EVENT);
            } else {
                list = this.getPreviousEvents().get();
            }
        }
        return list;
    }

    @Override
    public void clear() {
        try {
            this.lock.lock();
            this.pattern.clear();
        } finally {
            this.lock.unlock();
        }
    }
}
